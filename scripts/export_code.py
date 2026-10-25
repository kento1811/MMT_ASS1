import argparse
import fnmatch
import os

DEFAULT_IGNORES = [
    ".git",
    ".svn",
    ".hg",
    "__pycache__",
    "venv",
    "env",
    ".venv",
    "*.pyc",
    "*.pyo",
    "node_modules",
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    ".dart_tool",
    "build",
    "dist",
    "pubspec.lock",
    ".idea",
    ".vscode",
    "*.png",
    "*.jpg",
    "*.jpeg",
    "*.gif",
    "*.ico",
    "*.svg",
    "*.mp4",
    "*.mp3",
    "*.pdf",
    "*.zip",
    "*.tar.gz",
    "*.rar",
    "*.exe",
    "*.dll",
    "*.so",
    "*.dylib",
    ".DS_Store",
    "data/cache",
    "*.csv",
    "*.txt",
    "*.log",
]


def load_ignore_patterns(root_path):
    patterns = set(DEFAULT_IGNORES)
    for ignore_file in [".gitignore", ".dockerignore"]:
        ignore_path = os.path.join(root_path, ignore_file)
        if os.path.exists(ignore_path):
            try:
                with open(ignore_path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith(("#", "!")):
                            continue
                        if line in ["*", "**"]:
                            continue
                        line = line.removeprefix("/")
                        line = line.replace("\\", "/")
                        patterns.add(line)
            except OSError as e:
                print(f"⚠️ Không thể đọc {ignore_file}: {e}")
    return patterns


def is_ignored(path, patterns, root_path):
    rel_path = os.path.relpath(path, root_path)
    rel_path = rel_path.replace(os.sep, "/")
    parts = rel_path.split("/")

    for pattern in patterns:
        for part in parts:
            if fnmatch.fnmatch(part, pattern) or fnmatch.fnmatch(part + "/", pattern):
                return True
        if fnmatch.fnmatch(rel_path, pattern) or fnmatch.fnmatch(
            rel_path + "/", pattern
        ):
            return True
    return False


def write_file_content(file_path, rel_path, outfile):
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()

        std_rel_path = rel_path.replace(os.sep, "/")
        outfile.write("========\n")
        outfile.write(f"{std_rel_path}\n")
        outfile.write("========\n")
        outfile.write(content)
        outfile.write("\n\n")

        print(f" Đã gộp: {std_rel_path}")
        return True
    except UnicodeDecodeError:
        pass
    except OSError as e:
        print(f"⚠️ Lỗi đọc file {rel_path}: {e}")
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Export code from folders/files to a single text file."
    )
    parser.add_argument(
        "targets", nargs="+", help="Danh sách các thư mục hoặc file cần lấy code"
    )
    parser.add_argument(
        "-o", "--output", required=True, help="Tên file đầu ra (VD: output.txt)"
    )

    args = parser.parse_args()

    root_path = os.getcwd()
    ignore_patterns = load_ignore_patterns(root_path)

    file_count = 0

    with open(args.output, "w", encoding="utf-8") as outfile:
        for target in args.targets:
            target_path = os.path.abspath(target)

            if not os.path.exists(target_path):
                print(f"❌ Không tìm thấy thư mục/file: {target}")
                continue

            if os.path.isfile(target_path):
                if not is_ignored(
                    target_path, ignore_patterns, root_path
                ) and write_file_content(
                    target_path, os.path.relpath(target_path, root_path), outfile
                ):
                    file_count += 1

            elif os.path.isdir(target_path):
                for root, dirs, files in os.walk(target_path):
                    dirs[:] = [
                        d
                        for d in dirs
                        if not is_ignored(
                            os.path.join(root, d), ignore_patterns, root_path
                        )
                    ]

                    for file in files:
                        file_path = os.path.join(root, file)
                        if not is_ignored(
                            file_path, ignore_patterns, root_path
                        ) and write_file_content(
                            file_path, os.path.relpath(file_path, root_path), outfile
                        ):
                            file_count += 1

    print("-" * 30)
    if file_count == 0:
        print("⚠️ CẢNH BÁO: Không có file nào được xuất ra!")
    else:
        print(f"✅ HOÀN TẤT: Đã gộp {file_count} file thành công ra: {args.output}")


if __name__ == "__main__":
    main()