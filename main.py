"""
CLI entry point for running the Search Keyword Performance Processor locally.
Usage: python main.py <path_to_data_file>
"""

import sys
import os
from src.processor import SearchKeywordProcessor


def main():
    if len(sys.argv) < 2:
        print("Usage: python main.py <path_to_data_file>")
        sys.exit(1)

    input_file = sys.argv[1]

    if not os.path.exists(input_file):
        print(f"Error: File not found: {input_file}")
        sys.exit(1)

    with open(input_file, "r", encoding="utf-8") as f:
        file_content = f.read()

    processor = SearchKeywordProcessor()
    revenue_data = processor.process_file(file_content)
    date_str = processor.extract_date_from_content(file_content)
    filename, output_content = processor.generate_output(revenue_data, date_str)

    output_path = os.path.join(os.path.dirname(input_file), filename)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(output_content)

    print(f"\n✅ Output written to: {output_path}\n")
    print(output_content)


if __name__ == "__main__":
    main()
