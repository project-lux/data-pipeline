
KEYWORD_OPTIONS = ['case-insensitive','diacritic-insensitive','punctuation-insensitive','whitespace-sensitive','stemmed','wildcarded']
VALUES_OPTIONS = ['eager', 'concurrent']

class FunctionProxy(object):
    def __init__(self, *args, **kw):
        self.args = list(args)
        self.kw = kw

    def recurse_args(self, a):
        if type(a) in [int, float]:
            return str(a)
        elif type(a) == str:
            return f"'{a}'"
        elif type(a) in [list, tuple]:
            l = []
            for item in a:
                l.append(self.recurse_args(item))
            return f"[{','.join(l)}]"
        elif isinstance(a, FunctionProxy):
            return a.toPlan()
        else:
            print(f"Unknown arg in {self}: {a}")

    def toPlan(self):
        argstr = self.recurse_args(self.args)[1:-1]
        return f"cts.{self.__class__.__name__}({argstr})\n"


class AbstractBooleanQuery(FunctionProxy):
    def toPlan(self):
        # New lines before queries in arg1
        qs = []
        for q in self.args[0]:
            qs.append(f"\n{self.recurse_args(q)}")
        qstr = ",".join(qs)
        if len(self.args) > 1:
            argstr = self.recurse_args(self.args[1:])[1:-1]
            return f"cts.{self.__class__.__name__}([{qstr}], {argstr})\n"
        else:
            return f"cts.{self.__class__.__name__}([{qstr}])\n"

class orQuery(AbstractBooleanQuery):
    pass

class andQuery(AbstractBooleanQuery):
    pass

class notQuery(AbstractBooleanQuery):
    # notQuery(query)
    pass

class andNotQuery(AbstractBooleanQuery):
    # andNotQuery(positiveQuery, negativeQuery)
    # positiveQuery NOT negativeQuery
    pass

class boostQuery(AbstractBooleanQuery):
    # boostQuery(baseline, boosted)
    pass


class tripleRangeQuery(FunctionProxy):
    def __init__(self, preds=[], objs=[]):
        self.args = [[], preds, objs]

    def toPlan(self):
        # wrap objs: cts.values(cts.iriReference(), '', options, {query})
        subjs = "[]"
        preds = self.recurse_args(self.args[1])
        obj = self.recurse_args(self.args[2])
        wobj = f"cts.values(cts.iriReference(), '', {VALUES_OPTIONS}, {obj})"
        argstr = f"{subjs},{preds},{wobj}"
        return f"cts.tripleRangeQuery({argstr})\n"        


class kwTripleRangeQuery(tripleRangeQuery):
    def toPlan(self):
        # wrap the objs query in the fn.empty ternary op
        subjs = "[]"
        preds = self.recurse_args(self.args[1])
        obj = self.recurse_args(self.args[2])

        if isinstance(self.args[2], sem_iri):
            wobj = obj
        else:
            wobj1 = f"fn.empty(fn.head(cts.values(cts.iriReference(),'',{VALUES_OPTIONS},{obj})))"
            wobj2 = " ? 'hopefullyNonExistentValue1' : "
            wobj3 = f"cts.values(cts.iriReference(), '',{VALUES_OPTIONS},{obj})"
            wobj = wobj1 + wobj2 + wobj3

        argstr = f"{subjs},{preds},{wobj}"
        return f"cts.tripleRangeQuery({argstr})\n"

class triples(FunctionProxy):

    def toPlan(self):
        # wrap objs: cts.values(cts.iriReference(), '', options, {query})
        subjs = self.recurse_args(self.args[0])
        preds = self.recurse_args(self.args[1])

        swrap = "cts.values(cts.iriReference(), '', ['eager', 'concurrent'],"
        ssubj = f"{swrap}{subjs})"
        wobj = "[]"
        argstr = f"{ssubj},{preds},{wobj}"
        return f"cts.triples({argstr}).toArray().map(x=>sem.tripleObject(x))\n"  

class fieldWordQuery(FunctionProxy):
    def __init__(self, *args, **kw):
        FunctionProxy.__init__(self, *args, **kw)
        if type(self.args[0]) != list:
            self.args[0] = [self.args[0]]
        if len(self.args) == 2:
            self.args.append(KEYWORD_OPTIONS)

class fieldRangeQuery(FunctionProxy):
    pass

class fieldValueQuery(FunctionProxy):
    pass

class jsonPropertyRangeQuery(FunctionProxy):
    pass

class jsonPropertyValueQuery(FunctionProxy):
    pass

class jsonPropertyWordQuery(FunctionProxy):
    pass

class nearQuery(FunctionProxy):
    pass

class wordQuery(FunctionProxy):
    pass

class documentQuery(FunctionProxy):
    pass

class sem_iri(FunctionProxy):
    def toPlan(self):
        return f"sem.iri('{self.args[0]}')"

class Prefixed(FunctionProxy):
    def __init__(self, pfx, what):
        self.prefix = pfx
        self.what = what

    def toPlan(self):
        return f"{self.prefix}('{self.what}')"

class Prefixer():
    def lux(self, what):
        return Prefixed("lux", what)
    def crm(self, what):
        return Prefixed("crm", what)
    def la(self, what):
        return Prefixed("la", what)
    def skos(self, what):
        return Prefixed("skos", what)


