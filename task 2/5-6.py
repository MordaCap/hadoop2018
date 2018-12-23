import re
import string
import unicodedata
from mrjob.job import MRJob
from mrjob.protocol import TextProtocol, ReprProtocol

pattern_5 = re.compile(r'[\[\(\s\ \b]+[a-zA-Zа-яА-я][a-zа-я]\.') 
pattern_6 = re.compile(r'[\[\(\s\ \b]+[a-zA-Zа-яА-Я]\.[a-zа-я]\.')

class MyMRJob(MRJob):
    OUTPUT_PROTOCOL = ReprProtocol

class Abv(MyMRJob):

    threshold = 30
    pattern_compiler = pattern_6

    def mapper(self, _, line):
        for word in self.pattern_compiler.findall(line):
            processed_word = unicodedata.normalize(
                "NFKD", word.strip().replace("(", "").replace("[", "")
            )
            yield processed_word, 1
    
    def reducer(self, word, count):
        n_words = sum(count)
        if n_words > self.threshold:
            yield word, n_words


if __name__ == '__main__':
    
    Abv.run()