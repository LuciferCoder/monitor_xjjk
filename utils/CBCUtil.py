import base64

# 加密工具


# 密钥和IV
AES_SECRET_KEY = 'abcabcabcabcabc1'  # 此处16|24|32个字符,分别对应AES-128、AES-192和AES-256
IV = 'helloBrook2abcde'  # 和密钥等长

# padding算法
BS = len(AES_SECRET_KEY)
pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
unpad = lambda s: s[0:-ord(s[-1:])]


class AES_ENCRYPT:
    def __init__(self):
        self.key = AES_SECRET_KEY
        self.mode = AES.MODE_CBC

    # 加密函数
    def encrypt(self, text):
        cryptor = AES.new(self.key.encode("utf8"), self.mode, IV.encode("utf8"))
        self.ciphertext = cryptor.encrypt(bytes(pad(text), encoding="utf8"))
        # AES加密时候得到的字符串不一定是ascii字符集的，输出到终端或者保存时候可能存在问题，使用base64编码
        return base64.b64encode(self.ciphertext).decode("utf-8")

    # 解密函数
    def decrypt(self, text):
        decode = base64.b64decode(text)
        cryptor = AES.new(self.key.encode("utf8"), self.mode, IV.encode("utf8"))
        plain_text = cryptor.decrypt(decode)
        return unpad(plain_text).decode("utf-8")


def chinese_to_ascii(text):
    text2 = base64.b64encode(text.encode('utf-8')).decode('ascii')
    return text2


if __name__ == '__main__':
    aes_encrypt = AES_ENCRYPT()
    text = "hello123"
    e = aes_encrypt.encrypt(text)
    d = aes_encrypt.decrypt(e)
    print(text)
    print(e)
    print(d)