require 'json'
require 'set'
require 'zlib'

class FileStore

  MAX_VALUE_SIZE = 2 ** 32 - 1

  Record = Struct.new(:key, :value) do
    def <=>(r)
      key <=> r.key
    end

    def ==(r)
      r.key == key
    end
  end

  def initialize(root, index:, unwritten_log:, sparse_factor: nil)
    @root = root
    @sparse_factor = sparse_factor || 4096
    @index = index
    @write_sst = SortedSet.new
    @read_sst = @write_sst
    @mutex = Mutex.new
    @unwritten_log = unwritten_log
  end

  def self.open(root, sparse_factor: nil)
    index_path = File.join(root, 'index.json')
    if Dir.exist?(root)
      index = JSON.parse(File.read(index_path))
    else
      Dir.mkdir(root)
      Dir.mkdir(File.join(root, 'sst'))
      index = {}
    end
    begin
      File.open(File.join(root, 'unwritten.log'), 'a') do |unwritten_log|
        store = FileStore.new(root, index: index, unwritten_log: unwritten_log, sparse_factor: sparse_factor)
        yield store
      end
    ensure
      File.write(index_path, JSON.dump(index))
    end
  end

  def encode_int(i) [i].pack('N') end

  def decode_int(s) s.unpack('N').first end

  def write_kv(f, key, value)
    ksize = key.bytesize
    v_deflated = Zlib.deflate(value)
    vsize = v_deflated.bytesize # TODO: compression
    bytes_written = 0
    bytes_written += f.write(encode_int(ksize))
    bytes_written += f.write(key)
    bytes_written += f.write(encode_int(vsize))
    bytes_written + f.write(v_deflated)
  end

  def dump_sst!
    sst = @mutex.synchronize do
      s = @write_sst
      @write_sst = SortedSet.new
      s
    end
    f_name = File.join(@root, 'sst', Time.now.to_i.to_s)
    @index[f_name] = {}
    File.open(f_name, 'w') do |f|
      last_index_offset = -@sparse_factor
      offset = 0
      sst.each do |record|
        if offset >= last_index_offset + @sparse_factor
          @mutex.synchronize do
            @index[f_name][record.key] = offset
          end
          last_index_offset = offset
        end
        offset += write_kv(f, record.key, record.value)
      end
    end
    @read_sst = @write_sst
  end

  def put(key, value)
    if value.bytesize > MAX_VALUE_SIZE
      raise "Value is too large to store under a single key"
    end
    write_kv(@unwritten_log, key, value)
    r = Record.new(key, value)
    @mutex.synchronize do
      if @write_sst.include?(r)
        @write_sst.delete(r)
      end
      @write_sst.add(r)
    end
  end

  def get(key)
    record = @read_sst.find { |r| r.key == key }
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
        data = File.open(fname) do |f|
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

end