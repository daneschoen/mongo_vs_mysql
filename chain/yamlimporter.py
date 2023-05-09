import yaml
import re
from chain import ImporterLink

class YamlImporter(ImporterLink):

    @classmethod
    def dict_yaml_constructor(loader, node):
        return loader.construct_mapping(node)

    def next(self, db, meta, data):
        data.seek(0)
        if re.search('^---', (data.next()): 
            meta['type'] = 'yaml'
            db.meta.save(meta)
            return self.import_(db, meta, data)
        else:
            return self.call_next(db, meta, data) 

    def import_(self, db, meta, data):
        data.seek(0)
        yaml.add_constructor('!test_results', self.dict_yaml_constructor)
        yaml.add_constructor('!dictionary_result', self.dict_yaml_constructor)
        db.yaml.insert(yaml.load(data))
        meta['imported'] = True
        db.meta.save(meta)
        return True
