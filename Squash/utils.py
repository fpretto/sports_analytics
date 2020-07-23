#####################################################################################################################
#
# Purpose: Helper functions
# Creator: Fabricio Pretto
#
#  TODO:
#######################################################################################################################

def make_dir(directory):
    """
    Verifica si existen los directorios de destino. En caso de no existir, lo crea.

    """
    import os
    import shutil

    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)

