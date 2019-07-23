# general utility functions common to multiple classes

def is_in(keywords, string):
    # simple function to check if any keyword is in string
    # convert string and keywords to upper case before checking
    return any([string.upper().find(z.upper()) >= 0 for z in keywords])
