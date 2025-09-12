def load_text_file(file_path):
    """
    Loads and returns the content of a text file.

    Args:
        file_path (str): The path to the text file to be loaded.

    Returns:
        str: The content of the file as a string.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read()

