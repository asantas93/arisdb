require 'zlib'

class CompressedFile

  attr_accessor :inflate, :path

  def initialize(f)
    @underlying = f
    @inflate = Zlib::Inflate.new
    @buffered = ''
    @path = f.path
  end

  def seek(offset)
    @underlying.seek(offset)
  end

  def read_inflated(bytes=nil)
    if bytes
      inflated = ''
      until inflated.bytesize == bytes || (@underlying.pos == @underlying.size && (@buffered.nil? || @buffered.empty?))
        if @buffered.nil? || @buffered.empty?
          @buffered = @inflate.inflate(@underlying.read(4096))
        end
        remaining = bytes - inflated.bytesize
        inflated << @buffered.byteslice(0, remaining)
        @buffered = @buffered.byteslice(remaining..-1)
      end
      inflated
    else
      inflated, @buffered = @buffered << @inflate.inflate(@underlying.read), ''
      inflated
    end
  end

  def self.open(path)
    File.open(path) do |f|
      c = CompressedFile.new(f)
      begin
        yield c
      ensure
        c.inflate.close
      end
    end
  end

end
