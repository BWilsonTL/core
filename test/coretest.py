import sys
import unittest
sys.path.append("/Users/bewilson/PycharmProjects")
import pandas as pd
import core.data.frameformatting as ff


def typeerror():
    raise TypeError


class FormatterTest(unittest.TestCase):

    def setUp(self):
        pass

    test_frame = pd.DataFrame([[1, 1.3, 'A', None], [2, 2.3, 'B', 4]], columns=['int', 'float', 'str', 'NaN'])

    def test_catTest(self):
        self.assertFalse(ff._is_categorical(self.test_frame, 'str'))

    def test_objTest(self):
        self.assertTrue(ff._is_object_type(self.test_frame, 'str'))
        self.assertFalse(ff._is_object_type(self.test_frame, 'int'))

    def test_catCast(self):
        self.assertRaises(TypeError, lambda: ff.categorical_cast(self.test_frame, 'float'))

    def test_basisExpansionInt(self):
        self.assertRaises(TypeError, lambda: ff.basis_expansion(self.test_frame, 'NaN'))


if __name__ == '__main__':
    unittest.main()