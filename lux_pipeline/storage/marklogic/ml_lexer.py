import ply.lex as lex
import ujson as json
from .query_token_rules import *

import numpy as np
from .cts import Prefixer
from .cts import fieldRangeQuery, fieldWordQuery, andQuery, orQuery, andNotQuery, \
    notQuery, tripleRangeQuery, kwTripleRangeQuery, documentQuery, triples, sem_iri

prefs = Prefixer()

CONSTANTS = {
    'biography': '54e35d81-9548-4b4e-8973-de02b09bf9da',
    'description': 'b9d84f17-662e-46ef-ab8b-7499717f8337',
    'english': '1fda962d-1edc-4fd7-bfa9-0c10e3153449'

}

class BaseQuery():  
    pass

class SimilarQuery(BaseQuery):
    def __init__(self, value, qc, ml_store):
        self.query_class = qc
        self.value = value
        self.ml_store = ml_store

    def fetch_record(self):
        uri = self.value.value
        if not uri.startswith('https://lux.collections'):
            if len(uri.split('/')) == 2:
                # type/uuid
                uri = f"https://lux.collections.yale.edu/data/{uri}"
            else:
                # could be lux-front-xxx
                uri = uri.replace('lux-front-sbx', 'lux')
                uri = uri.replace('lux-front-tst', 'lux')
                uri = uri.replace('lux-front-dev', 'lux')
                uri = uri.replace('lux-front-prd', 'lux')
        rec = self.ml_store[uri]
        return rec

    def make_agent_query(self, rec):
        js = rec['json']
        andq = []
        orq = []
        if request['process_concepts']:
            for cxn in js.get('classified_as', []):
                if 'id' in cxn:
                    orq.append(f'classification(id={cxn["id"]})')
        txts = []
        wds = {}
        if request['process_keywords']:
            for stmt in js.get('referred_to_by', []):
                # English description, English or None biography
                if 'classified_as' in stmt:
                    for cxn in stmt['classified_as']:
                        if cxn['id'].endswith(CONSTANTS['biography']):
                            txts.append(stmt['content'])
                        elif cxn['id'].endswith(CONSTANTS['description']) and 'language' in stmt:
                            # Check if we're english
                            for lang in stmt['language']:
                                if lang['id'].endswith(CONSTANTS['english']):
                                    txts.append(stmt['content'])
            for txt in txts:
                txt = txt.replace('-', ' ')
                txt = txt.replace('.', ' ')
                txt = txt.replace(',', ' ')
                toks = txt.lower().split()
                for t in toks:
                    if len(t) > 3 and t.isalpha():
                        try:    
                            wds[t] += 1
                        except:
                            wds[t] = 1
            if wds:
                if 'born' in wds:
                    del wds['born']
                if 'died' in wds:
                    del wds['died']
                wdits = list(wds.items())
                wdits.sort(key=lambda x:x[1], reverse=True)
                if len(wdits) > 5:
                    wdits = wdits[:5]
                    for w in wdits:
                        orq.append(f'text="{w[0]}"')

        # FIXME: take into account dates before 0
        if 'born' in js and 'timespan' in js['born']:
            b = js['born']['timespan']['begin_of_the_begin'][:4]
        elif 'formed_by' in js and 'timespan' in js['formed_by']:
            b = js['formed_by']['timespan']['begin_of_the_begin'][:4]
        else:
            b = ""
        if b:
            while b[0] == '0':
                b = b[1:]
            b = int(b)
            if b > 1900:
                diff = 10
            elif b > 1700:
                diff = 20
            else:
                diff = 35
            andq.append(f"AND(bornTime>{b-diff},bornTime<{b+diff})")

        if 'died' in js and 'timespan' in js['died']:
            d = js['died']['timespan']['begin_of_the_begin'][:4]
        elif 'dissolved_by' in js and 'timespan' in js['dissolved_by']:
            d = js['dissolved_by']['timespan']['begin_of_the_begin'][:4]
        else:
            d = ""
        if d:
            while d[0] == '0':
                d = d[1:]
            d = int(d)
            if d > 1900:
                diff = 10
            elif d > 1700:
                diff = 20
            else:
                diff = 35
            andq.append(f"AND(diedTime>{d-diff},diedTime<{d+diff})")

        mofs = []
        if 'member_of' in js:
            for memb in js['member_of']:
                if 'id' in memb:
                    orq.append(f'memberOf(id={memb["id"]})')

        orqs = f"OR({','.join(orq)})"
        andqs = f"AND({orqs},{','.join(andq)})"
        return andqs

    def make_item_query(self, rec):
        pass

    def make_work_query(self, rec):
        pass

    def to_cts(self):
        rec = self.fetch_record()
        fn = getattr(self, f"make_{self.query_class.lower()}_query")
        qstr = fn(rec)
        print(qstr)
        p = Parser(self.ml_store)
        q = p.parse(self.query_class, qstr)
        return q.to_cts()





class LeafQuery(BaseQuery):
    def __init__(self, tag, qc, field):
        self.query_class = qc
        self.field = field
        self.searchtag = tag
        self.comp = None
        self.value = None
        self.options = {}

    def set_value(self, v):
        if self.value:
            self.value = f"{self.value} {v}"
        else:
            self.value = v

    def set_comparitor(self, c):
        if not c in ['=', '>=', '>', '<', '<=']:
            raise ValueError(f"Unknown comparitor {c}")
        self.comp = c

    def set_option(self, c, val):
        self.options[c] = val

    def to_cts(self):
        if self.field == "id":
            # make a sem.iri of the value
            v = self.value
            if not v.startswith('https://'):
                v = f"https://lux.collections.yale.edu/data/{v}"
            return sem_iri(v)
        elif self.query_class == "string":
            return fieldWordQuery(self.searchtag, self.value)
        elif self.query_class == "xstring":
            return fieldRangeQuery(self.searchtag, "=", self.value)
        elif self.query_class == "date": 
            # convert from actual date to long int
            val = int(np.datetime64(self.value).astype('<M8[s]').astype(np.int64))
            return fieldRangeQuery(self.searchtag, self.comp, val)
        elif self.query_class == "number":
            return fieldRangeQuery(self.searchtag, self.comp, self.value)
        else:
            raise ValueError(f"leaf has unknown / unprocessed value type {self.query_class}")

class BoolQuery(BaseQuery):
    def __init__(self, field):
        self.field = field
        self.queries = []

    def add_query(self, q):
        self.queries.append(q)

    def to_cts(self):
        if self.field.lower() == "or":
            qc = orQuery
        elif self.field.lower() == "and":
            qc = andQuery
        elif self.field.lower() == "not":
            qc = notQuery
            return qc(self.queries[0].to_cts())
        else:
            raise NotImplementedError(f"Unknown bool query type: {self.field}")

        if len(self.queries) < 2 and self.field.lower() != "not":
            raise ValueError('Must give 2 or more queries to boolean operators')
        elif self.field.lower() == 'not' and len(self.queries) != 1:
            raise ValueError('Must exactly 2 queries to NOT operator')

        # build subordinate queries
        qs = []
        for q in self.queries:
            qs.append(q.to_cts())
        return qc(qs)

class RelQuery(BaseQuery):
    def __init__(self, tag, qc, field):
        self.query_class = qc
        self.searchtag = tag
        self.field = field
        self.query = None

    def add_query(self, q):
        if self.query:
            raise ValueError("tried to add more than one query to a RelQuery")
        self.query = q

    def to_cts(self):
        q = self.query.to_cts()
        p,t = self.searchtag.split(':')
        pf = getattr(prefs, p)
        if isinstance(self.query, LeafQuery):
            # we're a kwTripleRangeQuery
            return kwTripleRangeQuery(preds=[pf(t)], objs=q)
        else:
            # we're a regular tripleRangeQuery
            return tripleRangeQuery(preds=[pf(t)], objs=q)

class InverseRelQuery(RelQuery):

    def to_cts(self):
        q = self.query.to_cts()
        print(self.searchtag)
        p,t = self.searchtag.split(':')
        pf = getattr(prefs, p)
        return documentQuery(triples(q, [pf(t)]))

class ConfiguredParser(object):
    def __init__(self, ml_store=None):
        with open('searchConfig.json') as fh:
            data = fh.read()
        js = json.loads(data)
        self.config = js
        self.all_rels = {}
        self.all_fields = {}
        self.per_class = {}
        self.process_config()

        self.ml_store = ml_store
        self.current = []
        self.current_class = []
        self.top = None

    def process_config(self):
        multiVals = self.config['*']
        for (k,v) in self.config.items():
            if k != '*':
                # process in our multis
                base = {'fields': {}, 'rels': {}}
                self.per_class[k] = base
                for (k2,v2) in multiVals.items():
                    nv2 = v2[0].replace('*', k.lower())
                    if v2[1] in ['string', 'xstring', 'date', 'number']:
                        base['fields'][k2] = [nv2, v2[1]]
                    else:
                        base['rels'][k2] = [nv2, v2[1]]
            for (k2, v2) in v.items():
                if v2[1] in ['string', 'xstring', 'date', 'number']:
                    self.all_fields[k2] = [v2, k]
                    if k != '*':
                        base['fields'][k2] = v2
                elif v2[1] in ['Item', 'Work', 'Agent', 'Place', 'Concept', 'Set', 'Reference', 'Activity']:
                    self.all_rels[k2] = [v2, k]
                    if k != '*':
                        base['rels'][k2] = v2
                else:
                    raise ValueError(f"Unknown triple object type: {v2[1]}")

    def is_relationship(self, value):
        cc = self.current_class[-1]
        if cc in self.per_class:
            return self.per_class[cc]['rels'].get(value, [None, False])
        else:
            return self.all_rels.get(value, [[None, False]])[0]

    def is_inverse_relationship(self, value):
        cc = self.current_class[-1]
        val = self.all_rels.get(value, [None, False])
        if val and val[0] and val[0][1] and val[0][1] == cc:
            self.current_class.append(val[1])
            return [val[0][0], val[1]]
        else:
            return [None, False]

    def is_field(self, value):
        cc = self.current_class[-1]
        if cc in self.per_class:
            return self.per_class[cc]['fields'].get(value, [None, False])
        else:
            return self.all_fields.get(value, [None, False])


class JsonParser(ConfiguredParser):

    def __init__(self, ml_store=None):
        ConfiguredParser.__init__(self, ml_store)
        self.booleans = ['AND', 'OR', 'NEAR', 'ANDNOT', 'BOOST']

    def split_clause(self, clause):
        keys = clause.keys()
        params = {}
        field = None
        value = None
        for k in keys:
            if k.startswith('_'):
                params[k] = clause[k]
            elif field is None:
                field = k
                value = clause[k]
            else:
                raise ValueError(f"Ambiguous clause with multiple types: {field} and {k}")
        return (field, value, params)           

    def process_node(self, node):
        (field, value, params) = self.split_clause(node)
        if type(value) == list:
            # Should be a boolean
            if not field in self.booleans:
                raise ValueError(f"Unknown Boolean {field}, or relationship with multiple values given")
            else:
                query = BoolQuery(field)    
                for val in value:
                    if type(val) != dict:
                        raise ValueError(f"A raw value cannot be a value in a Boolean: {val}")
                    else:
                        q = self.process_node(val)
                        query.add_query(q)
        elif type(value) == dict:
            # Should be a relationship
            if field == "similar":
                uri = value['id']
                query = SimilarQuery(uri, self.current_class[-1], self.ml_store)
            elif field[0] == "^":
                field = field[1:]
                relType = self.is_inverse_relationship(field)
                if relType[0] is None:
                    raise ValueError(f"Unknown relationship of __inverse__ field for " +
                        f"{self.current_class[-1]}: {field}")
                query = InverseRelQuery(relType[0], relType[1], field)
                q = self.process_node(value)
                query.add_query(q)     
            else:
                relType = self.is_relationship(field)
                if relType[1]:
                    query = RelQuery(relType[0], relType[1], field)
                    self.current_class.append(relType[1])
                    q = self.process_node(value)
                    query.add_query(q)
                    self.current_class.pop()
                else:
                    raise ValueError(f"Unknown relationship {field}")
        elif type(value) in [str, int, float]:
            # Should be a field
            fieldType = self.is_field(field)
            if fieldType[1]:
                query = LeafQuery(fieldType[0], fieldType[1], field)
                query.set_value(value)
                if '_comp' in params:
                    query.set_comparitor(params['_comp'])
                if '_stemmed' in params:
                    query.set_option('stemmed', params['_stemmed'])
            else:
                raise ValueError(f"Unknown field {field}")                
        return query

    def parse(self, query, topClass=None):
        if '_scope' in query:
            self.current_class = [query['_scope']]
            del query['_scope']
        elif topClass is not None:
            self.current_class = [topClass]
        else:
            self.current_class = ["Reference"]
        return self.process_node(query)


class StringParser(ConfiguredParser):

    def __init__(self, ml_store=None):
        ConfiguredParser.__init__(self, ml_store)
        self.state = None
        self.comps = []
        self.result = None
        self.next_token = None

    def peek(self):
        if self.next_token:
            return self.next_token
        else:
            token = self.token()
            self.next_token = token
            return token

    def token(self):
        if self.next_token:
            token = self.next_token
            self.next_token = None
            return token
        else:
            return self.lexer.token()

    def process_tokens(self):
        token = self.token()

        while token:
            if not token:
                return
            elif not self.state:
                self.current = []
                self.comps = []

            self.state = token.type
            value = token.value

            if self.comps and not self.state == "COMP":
                if not isinstance(self.current[-1], LeafQuery) and \
                    not isinstance(self.current[-1], SimilarQuery):
                    raise ValueError("Saw comparitor outside of a leaf node query")
                else:
                    comp = ''.join(self.comps)
                    self.current[-1].set_comparitor(comp)
                    self.comps = []

            if self.state == "BOOL":
                # make a boolean and then add queries to it
                bq = BoolQuery(value)
                if self.current:
                    self.current[-1].add_query(bq)
                else:
                    self.top = bq
                self.current.append(bq)
                lp = self.peek()
                if lp.type != "LPAREN":
                    raise ValueError('Boolean not followed by "("')
            elif self.state == "COMP":
                self.comps.append(value)
            elif self.state == "COMMA":
                # only valid in a bool, but test for completed LeafQuery
                if isinstance(self.current[-1], LeafQuery):
                    if self.current[-1].field:
                        # OR(name = fish, material(name=silver))
                        closed = self.current.pop() 

            elif self.state == "RPAREN":
                closed = self.current.pop()
                if isinstance(closed, RelQuery):
                    x = self.current_class.pop()
                elif isinstance(closed, LeafQuery):
                    # The paren is actually from the encapsulating query
                    closed = self.current.pop()
                    if isinstance(closed, RelQuery):
                        x = self.current_class.pop()

            elif self.state == "QUOTE":
                # consume all tokens until self.state == QUOTE
                # only valid in the value of a leaf
                if not isinstance(self.current[-1], LeafQuery) and \
                    not isinstance(self.current[-1], SimilarQuery):
                    raise ValueError("saw quote not in value of a leaf/similar query")

                # use position to extract the substring
                startPos = token.lexpos
                nxt = self.token()
                # Allow escaped quotes within quoted string
                while nxt:
                    if nxt.type == "QUOTE" and self.lexer.lexdata[nxt.lexpos-1] != '\\':
                        break
                    nxt = self.token()
                if nxt:
                    endPos = nxt.lexpos
                    value = self.lexer.lexdata[startPos+1:endPos].strip()
                    self.current[-1].set_value(value)
                else:
                    raise ValueError("Unbalanced quotes")

            elif self.state == "WORD":
                # could be:
                curr = self.current[-1] if self.current else None

                if curr and isinstance(curr, LeafQuery):
                    curr.set_value(value)
                else:
                    peek = self.peek()
                    if peek is not None and peek.type == 'LPAREN':
                        # relationship
                        lp = self.token() # chomp (
                        if value == "similar":
                            id_or_uri = self.token()
                            if id_or_uri.value == "id":
                                eq_or_uri = self.token()
                                if eq_or_uri.value == "=":
                                    uri = self.token()
                                else:
                                    uri = eq_or_uri
                            else:
                                uri = id_or_uri
                            sq = SimilarQuery(uri, self.current_class[-1], self.ml_store)
                            self.current.append(sq)
                            if curr:
                                curr.add_query(sq)
                            else:
                                self.top = sq

                        elif value[0] == "^":
                            value = value[1:]
                            relType = self.is_inverse_relationship(value)
                            if relType[0] is None:
                                raise ValueError(f"Unknown relationship of __inverse__ field for" +
                                    " {self.current_class[-1]}: {value}")
                            irq = InverseRelQuery(relType[0], relType[1], value)
                            self.current.append(irq)
                            if curr:
                                curr.add_query(irq)
                            else:
                                self.top = irq

                        else:
                            relType = self.is_relationship(value)
                            if relType[1]:
                                rq = RelQuery(relType[0], relType[1], value)
                                self.current_class.append(relType[1])
                                if curr:
                                    curr.add_query(rq)
                                else:
                                    self.top = rq
                                self.current.append(rq)  

                    elif peek is not None and peek.type in ['COMP', 'WORD', 'QUOTE']:
                        # field
                        fieldType = self.is_field(value)
                        if fieldType[1]:
                            # Make a LeafQuery and set the field
                            lq = LeafQuery(fieldType[0], fieldType[1], value)
                            if curr:
                                curr.add_query(lq)
                            else:
                                self.top = lq
                            self.current.append(lq)
                        else:
                            # either a typo or raw content
                            raise ValueError(f"Saw '{value}' but expected a field (id, name, text, etc)")

                    else:
                        # end of the stream
                        raise ValueError(f"end of stream, or raw term encountered at end of stream, but expected a field/value")

            token = self.token()

    def parse(self, query, topClass):
        lexer = lex.lex(module=query_token_rules)
        lexer.input(query)
        self.lexer = lexer
        self.current_class = [topClass]
        self.state = None
        self.top = None
        self.process_tokens()
        return self.top
