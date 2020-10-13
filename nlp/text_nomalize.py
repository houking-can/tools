# pip install Unidecode
from unidecode import unidecode
import re


def strQ2B(text):
    """把字符串全角转半角"""
    ss = []
    for s in text:
        tmp = ""
        for uchar in s:
            inside_code = ord(uchar)
            if inside_code == 12288:  # 全角空格直接转换
                inside_code = 32
            elif (inside_code >= 65281 and inside_code <= 65374):  # 全角字符（除空格）根据关系转化
                inside_code -= 65248
            tmp += chr(inside_code)
        ss.append(tmp)
    return ''.join(ss)


def normalize_text(text):
    text = text.replace('\u2013', '-')
    text = strQ2B(text)
    text = unidecode(text)
    text = text.strip()
    text = re.sub(r'\s+',' ',text)
    return text


if __name__ == "__main__":
    text = "你好ｐｙｔｈ  b\u00e9zout svm\u2013adaboost-based 2\u22121/2 360\u00b0 "
    print(text)
    text = normalize_text(text)
    print(text)
