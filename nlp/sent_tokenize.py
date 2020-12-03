import re


def sent_tokenize(text):
    EG = '@EG@'
    text = text.strip()
    text = re.sub('\s+', ' ', text)
    text = re.sub('-\s+', '', text)
    text = re.sub('etc\s*\.', ' etc. ', text)
    text = re.sub('e\s*\.\s*g\s*\.\s*', ' e.g. ', text)
    text = re.sub('et\s*al\s*\.\s*', ' et al. ', text)
    text = re.sub('i\s*\.\s*e\s*\.\s*', ' i.e. ', text)
    text = re.sub(',[\s|,]*,', ', ', text)
    text = re.sub('\.\s*,', '., ', text)
    text = re.sub('\s+', ' ', text)
    if text[:-1] not in ['.', '!', '?', ';']:
        text += ' . '
    if text[:-1] != ' ':
        text += ' '
    text = text.replace('e.g.', EG)
    sentences = []
    start = 0
    for i in range(0, len(text)-1):
        if text[i] in ['!', '?', ';'] or (text[i] == '.' and not text[i+1].isdigit()):
            sent = text[start:i + 1]
            sent = sent.strip()
            sent = sent.replace(EG, 'e.g.')
            if len(sent) >= 5:
                sentences.append(sent)
            start = i + 1
    return sentences


if __name__ == "__main__":
    for sent in sent_tokenize("""build a graph that represents the text, and interconnects
                                words or other text entities with meaningful
                                relations. Depending, 20.45% on the application at hand,
                                text units of various sizes and characteristics can be
                                added as vertices in the graph, e.g. words, collocations,
                                entire sentences, or others. Similarly, it is the
                                application that dictates the type of relations that are
                                used to draw connections between any two such vertices,
                                e.g. lexical or semantic relations, contextual
                                overlap, etc."""
                            ):
        print(sent)
