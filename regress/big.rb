#!/usr/bin/env ruby

require 'tmpdir'
require_relative '../lib/db'

Dir.mktmpdir do |tmp|
  root = File.join(tmp, 'db')
  FileStore.open(root) do |store|
    (1..(4096 * 1024)).to_a.shuffle.each do |i|
      store.put("key #{i}", "value #{i}")
    end
    puts store.get("key 49383")
    puts store.get("key 99999")
    store.wait!
    puts store.get("key 945348")
  end
end
