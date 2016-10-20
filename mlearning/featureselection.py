from sklearn.feature_selection import RFE
from sklearn.linear_model import LogisticRegression
from sklearn.decomposition import PCA
from sklearn import preprocessing
from sklearn.cross_validation import train_test_split



class FeatureCulling(object):

    def __init__(self, input_set, pred_set):
        self.i_set = input_set
        self.p_set = pred_set

    def training_split(self):



        pass

    def do_rfe(self):
        pass

    def do_pca(self):
        pass





fc = FeatureCulling()

fc.do_rfe(df)



