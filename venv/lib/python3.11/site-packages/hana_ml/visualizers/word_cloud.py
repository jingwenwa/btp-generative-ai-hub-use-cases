"""
WordCloud Visualization.

The following classes and functions are available:

    * :class:`WordCloud`
"""
#pylint: disable=broad-except, too-few-public-methods, redefined-outer-name, abstract-method
#pylint: disable=consider-using-f-string, bare-except
import logging
import sys
import subprocess

try:
    import wordcloud
except BaseException as error:
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'wordcloud'])
        import wordcloud
    except:
        logging.getLogger(__name__).error("%s: %s", error.__class__.__name__, str(error))
        pass

from hana_ml.text.tm import tf_analysis

class WordCloud(wordcloud.WordCloud):
    """
    Extended from wordcloud.WordCloud.
    """
    def build(self, data, content_column=None, lang=None):
        """
        Generate wordcloud.

        Parameters
        ----------
        data : DataFrame
            The input SAP HANA DataFrame.

        content_column : str, optional
            Specified the column to do wordcloud.

            Defaults to the first column.

        lang : str, optional
            Specify the language type. HANA cloud instance currently supports 'EN', 'DE', 'ES', 'FR' and 'RU'.
            If None, auto detection will be applied.

            Defaults to None.

        Examples
        --------
        >>> wordcloud = WordCloud(background_color="white", max_words=2000,
                                  max_font_size=100, random_state=42, width=1000,
                                  height=860, margin=2).build(data=data,
                                                              content_column="CONTENT",
                                                              lang='EN')
        >>> import matplotlib.pyplot as plt
        >>> plt.imshow(wordcloud, interpolation='bilinear')
        >>> plt.axis("off")
        """

        if content_column is None:
            content_column = data.columns[0]

        new_id_name = "ID"
        if content_column == new_id_name:
            new_id_name = "NEW_ID"
        new_cat_name = "CAT"
        if content_column == new_cat_name:
            new_cat_name = "NEW_CAT"
        data_ = data.select(content_column).add_id(new_id_name).add_constant(new_cat_name, 1)
        tfidf = tf_analysis(data=data_, lang=lang, enable_stopwords=False)

        frequencies = {}
        for _, row in tfidf[0].select(["TM_TERMS", "TM_TERM_TF_F"]).collect().iterrows():
            frequencies[row["TM_TERMS"]] = row["TM_TERM_TF_F"]
        return self.generate_from_frequencies(frequencies=frequencies)
