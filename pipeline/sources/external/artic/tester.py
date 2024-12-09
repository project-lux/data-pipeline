# python -i ./run-full.py 
# >>> tester = cfgs.external['artic']['tester']
# >>> tester.test_person()

class ArticTester():
    def __init__(self, config):
        self.mapper = config['mapper']
        self.fetcher = config['fetcher']

    def test_person(self):
        ident = "agents/36397"
        data = self.fetcher.fetch(ident)
        rec = self.mapper.transform(data,'Person')
        assert rec['data']['born']['timespan']['begin_of_the_begin'] == "1886-01-01T00:00:00", "Error in Birth Date"

    def test_activity(self):
        ident = "exhibitions/1699"
        data = self.fetcher.fetch(ident)
        rec = self.mapper.transform(data,'Activity')
        assert rec['data']['used_specific_object'][0]['type'] == "Type", "Error in Used Specific Object"
