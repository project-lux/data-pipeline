tokens = ('BOOL','LPAREN','RPAREN','COMMA','WORD','COLON','QUOTE', 'COMP')

# Regular expression rules for simple tokens
t_LPAREN  = r'\('
t_RPAREN  = r'\)'
t_COMMA = r','
t_BOOL = r'(OR|AND|NOT|NEAR|BOOST)'
t_QUOTE = r'"'
t_WORD = r'[^,"() \t\n=><]+'
t_COMP = r'(=|>|<)'
# Need to accumulate COMPs

# A string containing ignored characters (spaces,tabs, newlines)
t_ignore  = ' \t\n\r'

# Error handling rule
def t_error(t):
  print("Illegal character '%s'" % t.value[0])
  t.lexer.skip(1)