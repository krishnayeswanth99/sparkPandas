def new_func(vals, cols, func):
    return func({j:i for i,j in zip(vals,cols)})
