""" A collection of utils for the property graph schema inference."""

import numpy as np

def label_format(labels):
    """ converts the set of labels to the following format: 
        "label1:label2:...:labeln"
        
        Parameters
        ----------
        labels : Python set
            Set of labels {label1, label2, ..., labeln}
        
        Return
        ------
        lab : string
            The converted set of labels
    
    """
    
    lab = ":"
    lab = lab.join(sorted(labels))
    
    return lab
    
def convert_to_num(s):
    """ converts the string s to int or float if applicable.
    
    Parameters
    ----------
    s : str
        
    Returns
    -------
    convert :
        int, if s can be converted to int
        float, if s can be converted to float but not to int
        str, with convert = s otherwise
    
    """
    try:
        # try to convert to int
        convert=int(s)
        return convert
    except ValueError:
        try:
            # try to convert to float
            convert=float(s)
            if np.isnan(convert) or np.isinf(convert):
                convert = 0.0
            return convert
        except ValueError:
            # s cannot be converted to an int or to a float
            return s
