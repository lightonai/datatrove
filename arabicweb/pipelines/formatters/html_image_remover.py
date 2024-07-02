from datatrove.pipeline.formatters.base import BaseFormatter
import regex as re
from datatrove.data import Document


class ImageHtmlRemover(BaseFormatter):

    name = " ⚞ HTML Image placeholder remover"

    def __init__(self):
        """
        Formatter module to remove ugly image html placeholders from text

        """
        self.regex = re.compile(r">>IMAGE_\d+<<", flags=re.MULTILINE)

        super().__init__()

    def format(self, doc: str) -> str:
        try:
            return self.regex.sub("", doc, timeout=5)
        except TimeoutError:
            print("Regex substitution timed out.")
            return doc