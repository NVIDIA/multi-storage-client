#
# Sphinx configuration.
#
# https://www.sphinx-doc.org/en/master/usage/configuration.html
#

import importlib.metadata as importlib_metadata

multi_storage_client_package_name = "multi-storage-client"

project = multi_storage_client_package_name
release = importlib_metadata.version(multi_storage_client_package_name)
author = "NVIDIA Multi-Storage Client Team"
copyright = "NVIDIA Corporation"

# Extensions.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
]

# Themes.
html_theme = "furo"
html_static_path = ["_static"]
html_css_files = ["custom.css"]

# Syntax highlighting. `pygments_dark_style` is specific to the Furo theme.
pygments_style = "solarized-light"
pygments_dark_style = "solarized-dark"

# Line numbers.
viewcode_line_numbers = True

# Docstrings.
autoclass_content = "both"
autodoc_typehints = "both"
add_module_names = False
toc_object_entries_show_parents = "hide"

# Intersphinx.
intersphinx_mapping = {
    "opentelemetry-python": ("https://opentelemetry-python.readthedocs.io/en/latest", None),
    "python": ("https://docs.python.org/3", None),
}
