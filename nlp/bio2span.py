def bio2span(file):
    text = open(file).read()
    sentences = re.split('\n\n+', text)
    sentences = [each.split('\n') for each in sentences]
    span_entity = []
    for sent in sentences:
        entity = []
        tag = ""
        for word in sent:
            word = word.split()
            if len(word) != 4:
                continue
            if word[-1] == 'O':
                if entity != []:
                    span_entity.append([' '.join(entity),tag])
                entity = []
                tag = ""
                continue
            if word[-1] != tag:
                if entity != []:
                    span_entity.append([' '.join(entity),tag])
                entity = [word[0]]
                tag = word[-1]
            elif word[-1] == tag:
                entity.append(word[0])
        if entity != []:
            span_entity.append([' '.join(entity),tag])
    return span_entity
