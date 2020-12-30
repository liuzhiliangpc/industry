def remove_parentheses(entity):
    keys = {'［', '(', '[', '（'}
    symbol = {'］':'［', ')':'(', ']':'[', '）':'（'}
    stack = []
    remove = []
    for index, s in enumerate(entity):
        if s in keys:
            stack.append((s, index))
        if s in symbol:
            if not stack:continue
            temp_v, temp_index = stack.pop()
            if entity[index-1] == '\\':
                t = entity[temp_index-1:index+1]
                remove.append(t)
            else:
                remove.append(entity[temp_index:index+1])

    for r in remove:
        entity = entity.replace(r, '')
    return entity