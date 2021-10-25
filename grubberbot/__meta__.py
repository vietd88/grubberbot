# `name` is the name of the package as used for `pip install package`
name = "grubberbot"

# `path` is the name of the package for `import package`
path = name.lower().replace("-", "_").replace(" ", "_")

# Your version number should follow https://python.org/dev/peps/pep-0440 and
# https://semver.org
version = "0.1.dev0"
author = "Pawngrubber"
author_email = "pawngrubber@gmail.com"
description = "Grubberbot: The Discord bot"  # One-liner
url = ""  # your project homepage
license = "GNU General Public License v3.0"  # See https://choosealicense.com
