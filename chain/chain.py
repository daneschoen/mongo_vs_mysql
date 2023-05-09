
"""
importer.add_link(ApdImporter())
importer.add_link(CsvImporter())
importer.add_link(DefaultImporter())
"""

class ImporterChain(object):
    def __init__(self):
        self.links = []
        
    def add_link(self, link):
        if self.links:
            self.links[-1].set_next(link)
        self.links.append(link)
        return self
    
    def begin(self, *context):
        if self.links:
            self.links[0].next(*context)
        else:
            raise AttributeError("You must provide at least one link")

"""
This is the 'abstract' handler that the each of the concrete handlers in the chain must extend/implement
- ApdImporter(ImporterLink)
- CsvImporter(ImporterLink)
- DefaultImporter(ImporterLink)
"""
class ImporterLink(object):
    def set_next(self, link):
        self.next_ = link
        return self
    
    def call_next(self, *context):
        if not self.next_:
            raise AttributeError("No next link defined")
        return self.next_.next(*context)
    
    def next(self, *context):
        raise NotImplementedError
        
