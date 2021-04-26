#!/usr/bin/env ruby

require 'tmpdir'
require_relative '../lib/db'

Dir.mktmpdir do |tmp|
  root = File.join(tmp, 'db')
  FileStore.open(root) do |store|
    store.put('apple', 'a red-skinned fruit that tastes okay')
    store.put('orange', "it's orange")
    store.put('banana', 'yellow')
    store.initiate_dump!
    store.wait!
    store.put('orange', 'new vision for oranges')
    store.put('xylophone', '????')
    store.put('ack', 'ACK ATTACK')
    store.initiate_dump!
    store.wait!
    puts store.get('apple')
    puts store.get('orange')
    puts store.get('banana')
    puts store.get('xylophone')
    puts store.get('ack')
    store.compact!
    puts '--- post compaction ---'
    puts store.get('apple')
    puts store.get('orange')
    puts store.get('banana')
    puts store.get('xylophone')
    puts store.get('ack')
  end
end
