"""
This module handles the generation of the files that represent the
artifacts. Currently this is experimental code only.
"""
import os
from .filewriter_base import FileWriterBase
from ...config import ConfigConstants

class AMDPWriter(FileWriterBase):
    """
    This class writes a amdp file
    """

    def write_file(self, algorithm, amdp_name, replacements):
        """
        Write the amdp file from replacements
        """
        path = self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_ABAP)
        file_name = amdp_name + ConfigConstants.AMDP_FILE_EXTENSION

        template_file_name = ConfigConstants.AMDP_TEMPLATE_FILENAME
        if 'UnifiedClassification' in algorithm:
            template_file_name = ConfigConstants.AMDP_TEMPLATE_UNIFIED_CLASSIFICATION_FUNCTION_FILENAME
        if 'UnifiedRegression' in algorithm:
            template_file_name = ConfigConstants.AMDP_TEMPLATE_UNIFIED_REGRESSION_FUNCTION_FILENAME
        template_file_path = os.path.join(os.path.dirname(__file__), ConfigConstants.TEMPLATE_DIR,
                                          template_file_name)
        self.write_template(path, file_name, template_file_path, replacements)
