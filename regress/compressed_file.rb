#!/usr/bin/env ruby

require 'tmpdir'
require_relative '../lib/compressed_file'

Dir.mktmpdir do |tmp|
  cfile = File.join(tmp, 'compressed')
  data =(1..100_000).to_a.to_s
  puts data.bytesize
  File.write(cfile, Zlib.deflate(data))
  CompressedFile.open(cfile) do |f|
    puts data.byteslice(0, 100)
    puts f.read_inflated(100)
    puts data.byteslice(100, 10_000)[0..100]
    puts f.read_inflated(10_000)[0..100]
    puts data.byteslice(10_100, 600)
    puts f.read_inflated(600)
    puts data.byteslice(10_700..-1).bytesize
    puts f.read_inflated.bytesize
  end
end
