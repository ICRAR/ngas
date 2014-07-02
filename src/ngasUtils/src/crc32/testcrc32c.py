import crc32c, os

blocksize = 1024 ** 2
block = str(bytearray(os.urandom(blocksize)))
count = 10
crc = 0 # initial value

for i in range(count):
    crc = crc32c.crc32(block, crc)
    print "crc32c = %d" % crc