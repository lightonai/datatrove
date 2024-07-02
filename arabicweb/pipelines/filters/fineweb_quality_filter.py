from datatrove.pipeline.filters.base_filter import BaseFilter
from datatrove.pipeline.filters.gopher_repetition_filter import find_duplicates
from datatrove.pipeline.writers.disk_base import DiskWriter
from datatrove.utils.typeshelper import Languages
from datatrove.utils.word_tokenizers import load_word_tokenizer
from datatrove.data import Document


class FineWebQualityFilter(BaseFilter):
    name = "🍷 FineWeb Quality"

    def __init__(
        self,
        exclusion_writer: DiskWriter = None,
        line_punct_thr: float = 0.03,
        line_punct_exclude_zero: bool = False,
        short_line_thr: float = 0.67,
        short_line_length: int = 30,
        char_duplicates_ratio: float = 0.01,
        new_line_ratio: float = 0.3,
        language: str = Languages.english,
    ):
        super().__init__(exclusion_writer)
        self.line_punct_thr = line_punct_thr
        self.line_punct_exclude_zero = line_punct_exclude_zero
        self.short_line_threshold = short_line_thr
        self.short_line_length = short_line_length
        self.char_duplicates_ratio = char_duplicates_ratio
        self.new_line_ratio = new_line_ratio
        self.tokenizer = load_word_tokenizer(language)

    def filter(self, doc: Document) -> bool | tuple[bool, str]:
        from nltk import word_tokenize

        # This was adapted to an arabic dataset, we took into consideration the different possibilities for each punct mark
        end_stop_chars = ("؟", "?", ",", ".", "'", '"', "!")
        start_stop_chars = (".", "!", "،")
        lines = doc.text.splitlines()

        end_ratio = sum(1 for line in lines if line.endswith(end_stop_chars)) / len(lines)
        start_ratio = sum(1 for line in lines if line.startswith(start_stop_chars)) / len(lines)
        ratio = end_ratio + start_ratio

        if ratio <= self.line_punct_thr and not (ratio == 0 and self.line_punct_exclude_zero):
            return False, "line_punct_ratio"

        ratio = sum(1 for line in lines if len(line) <= self.short_line_length) / len(lines)
        if ratio >= self.short_line_threshold:
            return False, "short_line_ratio"

        non_empty_lines = [line for line in lines if line.strip() != ""]
        ratio = find_duplicates(non_empty_lines)[1] / len(doc.text.replace("\n", ""))

        if ratio >= self.char_duplicates_ratio:
            return False, "char_dup_ratio"

        words = self.tokenizer.word_tokenize(doc.text)
        new_line = doc.text.count("\n")
        if new_line / len(words) > self.new_line_ratio:
            return False, "list_ratio"

        return True
