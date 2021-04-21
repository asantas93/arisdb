#!/usr/bin/env ruby

require 'tmpdir'
require_relative '../lib/db'

Dir.mktmpdir do |tmp|
  root = File.join(tmp, 'db')
  FileStore.open(root) do |store|
    store.put('apple', 'a red-skinned fruit that tastes okay')
    store.put('orange', "it's orange")
    store.put('banana', 'yellow')
    puts store.get('apple')
    puts store.get('orange')
    puts store.get('banana')
    puts store.get('notakey')
    store.dump_sst!
    puts store.get('apple')
    puts store.get('orange')
    puts store.get('banana')
    puts store.get('notakey')
  end
end
