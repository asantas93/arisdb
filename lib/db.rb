require 'json'
require 'set'
require 'zlib'

def nano_s
  (Time.now.to_r * 10 ** 9).to_i.to_s
end

class FileStore

  attr_accessor :active_log

  MAX_VALUE_SIZE = 2 ** 32 - 1

  Record = Struct.new(:key, :value) do
    def <=>(r)
      key <=> r.key
    end

    def ==(r)
      r.key == key
    end
  end

  def initialize(root, index:, active_log:, sparse_factor: nil, dump_threshold: nil)
    @root = root
    @sparse_factor = sparse_factor || 4096
    @dump_threshold = dump_threshold || @sparse_factor * 1024
    @index = index
    @write_sst = SortedSet.new
    @write_sst_size = 0
    @dumping_ssts = Set.new
    @mutex = Mutex.new
    @active_log = active_log
    @jobs = Set.new
  end

  def self.open(root, sparse_factor: nil, dump_threshold: nil)
    index_path = File.join(root, 'index.json')
    if Dir.exist?(root)
      index = JSON.parse(File.read(index_path))
    else
      Dir.mkdir(root)
      Dir.mkdir(File.join(root, 'sst'))
      Dir.mkdir(File.join(root, 'unwritten'))
      index = {}
    end
    begin
      old_logs = Dir.glob(File.join(root, 'unwritten', '*'))
      active_log = File.open(File.join(root, 'unwritten', nano_s), 'w')
      store = FileStore.new(root,
                            index: index,
                            active_log: active_log,
                            sparse_factor: sparse_factor,
                            dump_threshold: dump_threshold)
      begin
        old_logs.sort.each do |log|
          File.open(log) do |f|
            while (k, v = store.read_kv(f))
              store.put(k, v)
            end
          end
          File.delete(log)
        end
        yield store
      ensure
        store.active_log.close
      end
    ensure
      File.write(index_path, JSON.dump(index))
    end
  end

  def encode_int(i) [i].pack('N') end

  def decode_int(s) s.unpack('N').first end

  def write_kv(f, key, value)
    ksize = key.bytesize
    # TODO: compress entire file, using Zlib::FULL_FLUSH at index points
    v_deflated = Zlib.deflate(value)
    vsize = v_deflated.bytesize
    bytes_written = 0
    bytes_written += f.write(encode_int(ksize))
    bytes_written += f.write(key)
    bytes_written += f.write(encode_int(vsize))
    bytes_written + f.write(v_deflated)
  end

  def sst_path(name)
    File.join(@root, 'sst', name)
  end

  def new_sst_path
    sst_path(nano_s)
  end

  def dump_sst!(sst, log)
    if sst.any?
      # FIXME: atomic write
      f_name = new_sst_path
      File.open(f_name, 'w') do |f|
        write_records(sst, f)
      end
    end
    File.delete(log.path)
    @dumping_ssts.delete(sst)
  end

  def write_records(records, f)
    file_index = {}
    last_index_offset = -@sparse_factor
    offset = 0
    records.each do |record|
      if offset >= last_index_offset + @sparse_factor
        file_index[record.key] = offset
        last_index_offset = offset
      end
      offset += write_kv(f, record.key, record.value)
    end
    @index[File.basename(f.path)] = file_index
  end

  def initiate_dump!
    @write_sst_size = 0
    s = @write_sst
    @write_sst = SortedSet.new
    @dumping_ssts.add(s)
    @active_log.close
    sst_log = @active_log
    @active_log = File.open(File.join(@root, 'unwritten', nano_s), 'w')
    @jobs << Thread.new(s, sst_log) do |sst, log|
      dump_sst!(sst, log)
      @mutex.synchronize { @jobs.delete(Thread.current) }
    end
  end

  def put(key, value)
    if value.bytesize > MAX_VALUE_SIZE
      raise "Value is too large to store under a single key"
    end
    r = Record.new(key, value)
    @mutex.synchronize do
      @write_sst_size += write_kv(@active_log, key, value)
      if @write_sst.include?(r)
        @write_sst.delete(r)
      end
      @write_sst.add(r)
      if @write_sst_size >= @dump_threshold
        initiate_dump!
      end
    end
  end

  def get(key)
    record = @write_sst.find { |r| r.key == key }
    unless record
      @dumping_ssts.reverse_each do |sst|
        sst.each do |r|
          if r.key == key
            record = r
            break
          end
          break if record
        end
      end
    end
    if record
      record.value
    else
      ranges = @mutex.synchronize do
        # TODO: ordered collection for faster index lookup
        @index.flat_map do |fname, sparse_index|
          lower = sparse_index.select { |k, _| k <= key }.max
          upper = sparse_index.select { |k, _| k > key }.min
          if lower.nil?
            []
          else
            [[fname, [lower, upper]]]
          end
        end
      end
      ranges.reverse_each do |fname, range|
        lower, upper = range
        start = lower[1]
        data = File.open(sst_path(fname)) do |f|
          f.seek(start)
          if upper.nil?
            f.read
          else
            finish = upper[1]
            f.read(finish - start)
          end
        end
        offset = 0
        while offset < data.bytesize
          key_size = decode_int(data.byteslice(offset...offset += 4))
          k = data.byteslice(offset...offset += key_size)
          value_size = decode_int(data.byteslice(offset...offset += 4))
          value = data.byteslice(offset...offset += value_size)
          if k == key
            return Zlib.inflate(value)
          elsif k > key
            break
          end
        end
      end
      nil
    end
  end

  def wait!
    while @jobs.any?
      thread = @mutex.synchronize do
        @jobs.first
      end
      thread.join
    end
  end

  def compact!
    path = new_sst_path
    # TODO: don't always merge all SSTs?
    to_merge = Dir.glob(File.join(@root, 'sst', '*'))
    if to_merge.size > 1
      File.open(path, 'w') do |new|
        merge_ssts!(to_merge, new)
      end
    end
  end

  def merge_ssts!(unopened, new, opened: [])
    if unopened.any?
      to_open = unopened.pop
      File.open(to_open) do |f|
        opened << f
        merge_ssts!(unopened, new, opened: opened)
        @mutex.synchronize { @index.delete(File.basename(f.path)) }
      end
      File.delete(to_open)
    else
      File.open(new, 'w') do |f|
        _merge_ssts!(opened, f)
      end
    end
  end

  MergeRecord = Struct.new(:key, :value, :file) do
    def <=>(other)
      k_comp = key <=> other.key
      if k_comp == 0
        other.file.path <=> file.path
      else
        k_comp
      end
    end
  end

  def _merge_ssts!(files, new)
    current = files.map do |f|
      k, v = read_kv(f)
      MergeRecord.new(k, v, f)
    end
    to_write = Enumerator.new do |yielder|
      while current.any? do
        min = current.min
        yielder << Record.new(min.key, min.value)
        dups = current.select { |rec| rec.key == min.key }
        dups.each do |rec|
          rec.key, rec.value = read_kv(rec.file)
          if rec.key.nil?
            current.delete(rec)
          end
        end
      end
    end
    write_records(to_write, new)
  end

  def read_kv(f)
    key_size_bytes = f.read(4)
    if key_size_bytes.nil?
      nil
    else
      key_size = decode_int(key_size_bytes)
      key = f.read(key_size)
      value_size = decode_int(f.read(4))
      value = Zlib.inflate(f.read(value_size))
      [key, value]
    end
  end

end