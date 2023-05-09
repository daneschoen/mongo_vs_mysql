from pymongo.son_manipulator import SONManipulator

class MetaIdInjector(SONManipulator):
    """Inject the meta id of the datafile from which this document is being inserted."""
    def set_meta_id(self, meta_id):
        self.meta_id = meta_id
        
    def transform_incoming(self, son, collection):
        if self.meta_id and collection.name != 'meta':
            son['_meta_id'] = self.meta_id
        return son
    
    