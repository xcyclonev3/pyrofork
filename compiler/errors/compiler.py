#  Pyrofork - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
#
#  This file is part of Pyrofork.
#
#  Pyrofork is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrofork is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrofork.  If not, see <http://www.gnu.org/licenses/>.

import ast
import csv
import os
import re
import shutil

HOME = "compiler/errors"
DEST = "pyrogram/errors/exceptions"
NOTICE_PATH = "NOTICE"


def snek(s):
    # https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).lower()


def caml(s):
    s = snek(s).split("_")
    return "".join([str(i.title()) for i in s])

def get_classes_from_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())

    classes = [node.name for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
    return classes


# pyrogram.errors exports one class per name. When an error id exists under several codes, the class of the code
# module imported last wins. The order used to follow os.listdir(), which differs between machines; keep the order of
# the 2.3.69 wheel published on PyPI, so that the exported classes stay the same. Codes not listed are imported first.
IMPORT_ORDER = [
    "420_FLOOD", "303_SEE_OTHER", "406_NOT_ACCEPTABLE", "403_FORBIDDEN", "500_INTERNAL_SERVER_ERROR",
    "401_UNAUTHORIZED", "400_BAD_REQUEST", "503_SERVICE_UNAVAILABLE"
]


def import_rank(file_name):
    stem = file_name.rsplit(".", 1)[0]
    return IMPORT_ORDER.index(stem) if stem in IMPORT_ORDER else -1


def start():
    shutil.rmtree(DEST, ignore_errors=True)
    os.makedirs(DEST)

    files = sorted(os.listdir("{}/source".format(HOME)), key=lambda f: (import_rank(f), f))

    # Explicit class names (code, id -> name) for ids added under a second code, so that they don't take over the
    # name of the existing class and change what an existing "except" catches.
    with open("{}/class_names.tsv".format(HOME), encoding="utf-8") as f:
        class_names = {(code, error_id): name for code, error_id, name in list(csv.reader(f, delimiter="\t"))[1:]}

    with open(NOTICE_PATH, encoding="utf-8") as f:
        notice = []

        for line in f.readlines():
            notice.append("# {}".format(line).strip())

        notice = "\n".join(notice)

    with open("{}/all.py".format(DEST), "w", encoding="utf-8") as f_all:
        f_all.write(notice + "\n\n")
        f_all.write("count = {count}\n\n")
        f_all.write("exceptions = {\n")

        count = 0

        for i in files:
            code, name = re.search(r"(\d+)_([A-Z_]+)", i).groups()

            f_all.write("    {}: {{\n".format(code))

            init = "{}/__init__.py".format(DEST)

            if not os.path.exists(init):
                with open(init, "w", encoding="utf-8") as f_init:
                    f_init.write(notice + "\n\n")

            with open(init, "a", encoding="utf-8") as f_init:
                f_init.write("from .{}_{} import *\n".format(name.lower(), code))

            with open("{}/source/{}".format(HOME, i), encoding="utf-8") as f_csv, \
                open("{}/{}_{}.py".format(DEST, name.lower(), code), "w", encoding="utf-8") as f_class:
                reader = csv.reader(f_csv, delimiter="\t")

                super_class = caml(name)
                name = " ".join([str(i.capitalize()) for i in re.sub(r"_", " ", name).lower().split(" ")])

                sub_classes = []

                f_all.write("        \"_\": \"{}\",\n".format(super_class))

                for j, row in enumerate(reader):
                    if j == 0:
                        continue

                    count += 1

                    if not row:  # Row is empty (blank line)
                        continue

                    error_id, error_message = row

                    sub_class = caml(re.sub(r"_X", "_", error_id))
                    sub_class = re.sub(r"^2", "Two", sub_class)
                    sub_class = re.sub(r" ", "", sub_class)
                    sub_class = class_names.get((code, error_id), sub_class)

                    f_all.write("        \"{}\": \"{}\",\n".format(error_id, sub_class))

                    sub_classes.append((sub_class, error_id, error_message))

                with open("{}/template/class.txt".format(HOME), "r", encoding="utf-8") as f_class_template:
                    class_template = f_class_template.read()

                    with open("{}/template/sub_class.txt".format(HOME), "r", encoding="utf-8") as f_sub_class_template:
                        sub_class_template = f_sub_class_template.read()

                    class_template = class_template.format(
                        notice=notice,
                        super_class=super_class,
                        code=code,
                        docstring='"""{}"""'.format(name),
                        sub_classes="".join([sub_class_template.format(
                            sub_class=k[0],
                            super_class=super_class,
                            id="\"{}\"".format(k[1]),
                            docstring='"""{}"""'.format(k[2])
                        ) for k in sub_classes])
                    )

                f_class.write(class_template)

            f_all.write("    },\n")

        f_all.write("}\n")
        with open(init, "a", encoding="utf-8") as f_init:
            f_init.write("\n")
            all_classes = []
            for i in files:
                code, name = re.search(r"(\d+)_([A-Z_]+)", i).groups()
                classes = get_classes_from_file("{}/{}_{}.py".format(DEST, name.lower(), code))
                for j in classes:
                    if j not in ["BaseException", "Exception", "PyrogramException"]:
                        all_classes.append(j)
            f_init.write("__all__ = [\n")
            all_classes = sorted(set(all_classes))
            for i in all_classes:
                f_init.write("    \"{}\",\n".format(i))
            f_init.write("]\n")

    with open("{}/all.py".format(DEST), encoding="utf-8") as f:
        content = f.read()

    with open("{}/all.py".format(DEST), "w", encoding="utf-8") as f:
        f.write(re.sub("{count}", str(count), content))


if "__main__" == __name__:
    HOME = "."
    DEST = "../../pyrogram/errors/exceptions"
    NOTICE_PATH = "../../NOTICE"

    start()
