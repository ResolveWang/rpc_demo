class Encoder:

    @staticmethod
    def zigzag_encode(num):
        return num << 1 if num >= 0 else -2*num - 1

    @staticmethod
    def zigzag_decode(num):
        return num >> 1 if num & 1 == 0 else -((num+1) >> 1)

    @staticmethod
    def varint_encode(num):
        num = Encoder.zigzag_encode(num)
        mask = 0x7f
        rv = num & mask
        num >>= 7
        i = 1
        while num:
            rv |= (num & mask | 0x80) << (i * 8)
            num >>= 7
            i += 1
        return rv

    @staticmethod
    def varint_decode(num):
        mask = 0x7f
        rv = 0
        i = 0
        while num:
            rv |= (num & mask) << (7*i)
            i += 1
            num >>= 8
        return Encoder.zigzag_decode(rv)

