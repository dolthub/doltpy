from pallets_sphinx_themes import get_version
from pallets_sphinx_themes import ProjectLink

# Project --------------------------------------------------------------

project = "Doltpy"
copyright = ""
author = "Max"
release, version = get_version("Doltpy")

# General --------------------------------------------------------------

master_doc = "index"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "pallets_sphinx_themes",
    "sphinx_issues",
    "sphinxcontrib.log_cabinet",
    "sphinx_markdown_builder",
]
intersphinx_mapping = {"python": ("https://docs.python.org/3/", None)}
# issues_github_path = ""

# HTML -----------------------------------------------------------------

html_theme = "werkzeug"
html_context = {
    "project_links": [
        ProjectLink("Doltpy Website", "https://dolthub.com"),
        ProjectLink("PyPI releases", "https://pypi.org/project/doltpy/"),
        ProjectLink("Source Code", "https://github.com/dolthub/doltpy/"),
        ProjectLink("Issue Tracker", "https://github.com/dolthub/doltpy/issues/"),
    ]
}
html_sidebars = {
    "index": ["project.html", "localtoc.html", "searchbox.html"],
    "**": ["localtoc.html", "relations.html", "searchbox.html"],
}
singlehtml_sidebars = {"index": ["project.html", "localtoc.html"]}
# html_static_path = ["_static"]
# html_favicon = "_static/favicon.ico"
# html_logo = "_static/werkzeug.png"
html_title = f"Doltpy Documentation ({version})"
html_show_sourcelink = False

# LaTeX ----------------------------------------------------------------

latex_documents = [(master_doc, f"Doltpy-{version}.tex", html_title, author, "manual")]
