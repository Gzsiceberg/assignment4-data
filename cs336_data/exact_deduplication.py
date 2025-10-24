import os


def exact_deduplication(input_files, output_dir):
    max_lines = 100_000_000
    line_count = [0] * max_lines
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for path in input_files:
        with open(path, "r") as file:
            for line in file:
                line_hash = hash(line) % max_lines
                line_count[line_hash] += 1
    for path in input_files:
        output_path = os.path.join(output_dir, os.path.basename(path))
        with open(path, "r") as infile, open(output_path, "w") as outfile:
            for line in infile:
                line_hash = hash(line) % max_lines
                if line_count[line_hash] == 1:
                    outfile.write(line)


if __name__ == "__main__":
    input_files = [
        "data/doc3.txt",
        # Add more input file paths as needed
        "data/doc4.txt",
    ]
    output_dir = "data/deduplicated_output/"
    exact_deduplication(input_files, output_dir)
