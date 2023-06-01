from abc import ABC, abstractmethod

# noinspection PyUnresolvedReferences
from org.apache.lucene.search import CollectionStatistics, TermStatistics
# noinspection PyUnresolvedReferences
from org.apache.lucene.index import FieldInvertState
# noinspection PyUnresolvedReferences
from org.apache.lucene.search.similarities import BM25Similarity as LuceneBM25Similarity
# noinspection PyUnresolvedReferences
from org.apache.lucene.search.similarities import Similarity as LuceneSimilarity

BM25Similarity = LuceneBM25Similarity


class Similarity(LuceneSimilarity, ABC):

    @abstractmethod
    def computeNorm(self, state: FieldInvertState) -> float:
        pass

    @abstractmethod
    def scorer(self, boost: float, collectionStats: CollectionStatistics, *termStats: TermStatistics) -> float:
        pass


class ConstantSimilarity(Similarity):
    def computeNorm(self, state: FieldInvertState) -> float:
        return 1.0

    def scorer(self, boost: float, collectionStats: CollectionStatistics, *termStats: TermStatistics) -> float:
        return 1.0
