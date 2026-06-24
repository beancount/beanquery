import itertools
import re        
import os
import shutil

from pathlib import Path
from jinja2 import Environment, FileSystemLoader

# Convert Google-style docstrings to reStructuredText
from sphinx.ext.napoleon.docstring import GoogleDocstring

from beanquery import types
import beanquery.query_env as qe
import beanquery.query_compile as qc

from beanquery.query_compile import FUNCTIONS
from beanquery.query_env import TYPE_CATEGORIES
from beanquery.sources.beancount import EntriesTable, PostingsTable


# Category information for help display. Tuples: (title, description)
CATEGORY_INFO = {
    'amount': ("Amount and Commodity Functions", "An amount is a value with a currency/commodity."),
    'account': ("Account Functions", ""),
    'position': ("Position & Inventory Functions", "A position is a single amount held at cost.\n\nExample: 10 HOOL {100.30 USD}\n\nA collection of multiple positions is an inventory"),
    'metadata': ("Access metadata", "Access metadata from postings, transactions, and accounts."),
    'date': ("Date Functions", ""),
    'atomic': ("Atomic Functions", "Work on basic types: strings, numbers, etc.")
}

# ============================================================================
# Low-level RST formatting helpers
# ============================================================================

def convert_docstring_to_rst(docstring):
    """Convert Google-style docstring to RST."""
    google_doc = GoogleDocstring(docstring)
    return '\n'.join(google_doc.lines())


def extract_first_sentence(text):
    """Extract the first sentence from text."""
    if not text:
        return ""
    first_part = text.split('.')[0]
    return f"{first_part.strip()}." if first_part else ""


# ============================================================================
# Function documentation generation
# ============================================================================

def group_function_variants(functions):
    """Group function variants by their name.
    
    Args:
        functions: List of (name, doc, args) tuples.
    
    Returns:
        Iterator of (name, list[(name, doc, args)] ) tuples.
    """
    for name, group in itertools.groupby(functions, key=lambda x: x[0]):
        yield name, [ g[1:] for g in group]


def preprocess_function_with_variant_docs(function_name, variants):
    """Preprocess function with multiple docstrings into structured data for Jinja2.

    Args:
        function_name: Name of the function
        variants: List of variants of this function; (doc, args, outtype) tuples
    
    Returns:
        List of dicts, each containing:
        - 'signatures': formatted block of function signatures
        - 'doc_text': RST documentation string
    
    Example of one element:
    
        dict(signatures =
            "myfunc(a: str)\\n"
            "myfunc(a: str, default: str)",
            doc_text = "Here goes the documentation for both function variants"
        )
    
    """
    variant_groups = []
    
    # Make a joint documentation for all functions of equal name and docstring:
    for doc, entries in itertools.groupby(variants, key=lambda x: x[0]):
        entries_list = list(entries)
        
        # Format signatures into a code block
        signatures = [f"{function_name}({args})" + (f" -> {outtype}" if outtype else "")
                      for _, args, outtype in entries_list]
        sig_block = "\n    ".join(signatures)
        
        doc_text = convert_docstring_to_rst(doc) if doc else ''
        
        variant_groups.append({
            'signatures': sig_block,
            'doc_text': doc_text
        })
    
    return variant_groups


def preprocess_function_with_single_doc(function_name, variants):
    """Preprocess function with single docstring into structured data for Jinja2.
    
    Args:
        function_name: Name of the function
        variants: List of variants of for this function; (doc, args, outtype) tuples
    
    Returns:
        Dict containing:
        - 'signatures': list of signature strings
        - 'doc_text': RST documentation string
    """

    signatures = [f"{function_name}({args})" + (f" -> {outtype}" if outtype else "")
                   for _, args, outtype in variants]
    
    unique_docs = {doc for doc, _, _ in variants if doc}
    doc_text = ''
    if unique_docs:
        shared_doc = list(unique_docs)[0]
        doc_text = convert_docstring_to_rst(shared_doc)
    
    return {
        'signatures': signatures,
        'doc_text': doc_text
    }


def preprocess_function_documentation(functions):
    """Preprocess functions into structured data for Jinja2 template.
    
    Args:
        functions: List of (name, doc, args) tuples from _describe_functions().
    
    Returns:
        List of tuples (name, args, has_multiple_docs, docs) where:
        - name: function name (str)
        - args: '...' if we have multiple function variants, else list of
          all arg strings (str | list[str])
        - has_multiple_docs: boolean indicating if function has multiple docstrings
        - docs: list[dict] if multiple docs (each dict has 'signatures' and 'doc_text'),
          else str with doc_text if single doc
    """
    result = []
    
    for function_name, variants in group_function_variants(functions):
        variants_list = list(variants)
        
        unique_docs = {doc for doc, _, _ in variants_list if doc}
        has_multiple_docstrings = len(unique_docs) > 1
        
        if has_multiple_docstrings:
            docs = preprocess_function_with_variant_docs(function_name, variants_list)
            result.append((function_name, '...', True, docs))
        else:
            docs = preprocess_function_with_single_doc(function_name, variants_list)
            result.append((function_name, docs['signatures'], False, docs['doc_text']))
    
    return result


def preprocess_function_summary(functions):
    """Preprocess function list into structured data for summary rendering.
    
    Args:
        functions: List of (name, doc, args) tuples from _describe_functions().
    
    Returns:
        List of tuples (function_name, first_sentence) where first_sentence
        may be empty string if no docstring available.
    """
    result = []
    
    for function_name, variants in group_function_variants(functions):
        # Extract first sentence from docstring if available
        unique_docs = {doc for doc, _, _ in variants if doc}
        first_sentence = ''
        if unique_docs:
            first_sentence = extract_first_sentence(list(unique_docs)[0])
        
        result.append((function_name, first_sentence))
    
    return result

def preprocess_targets(table_class):
    """Preprocess table columns into structured data for Jinja2 template.
    
    Args:
        table_class: Table subclass with a columns attribute.
    
    Returns:
        List of tuples (name, type_name, doc) for each column.
    """
    result = []
    
    for name, column in table_class.columns.items():
        # Clean up docstring whitespace
        doc = re.sub(r'[ \n\t]+', ' ', column.__doc__ or '').strip()
        type_name = types.name(column.dtype)
        result.append((name, type_name, doc))
    
    return result


# ============================================================================
# Template rendering
# ============================================================================

def create_global_context():
    """Create global context with all functions and variables for templates.
    
    Returns:
        Dictionary containing all functions and variables templates can use.
    """
    return {
        # Helper functions
        'preprocess_function_documentation': preprocess_function_documentation,
        'preprocess_function_summary': preprocess_function_summary,
        'preprocess_targets': preprocess_targets,
        
        # Data access functions
        '_describe_functions': qe._describe_functions,
        
        # Constants and data
        'FUNCTIONS': qc.FUNCTIONS,
        'TYPE_CATEGORIES': TYPE_CATEGORIES,
        'CATEGORY_INFO': CATEGORY_INFO,
        'PostingsTable': PostingsTable,
        'EntriesTable': EntriesTable,
    }


def render_templates(docs_dir, template_dir):
    """Render Jinja2 files to Sphinx (RST) documentation.
    
    Args:
        env: Jinja2 Environment with global context.
        docs_dir: Output directory path.
        template_dir: Directory containing template (*.j2) files.
    """
    configs = []
    
    # Scan for all .j2 template files
    for template_file in sorted(template_dir.glob("*.j2")):
        template_name = template_file.name
        # Output filename: remove .j2 extension
        output_filename = template_name[:-3]
        
        configs.append((template_name, output_filename))

    env = Environment(
        loader=FileSystemLoader(template_dir),
        trim_blocks=True,
        lstrip_blocks=True
    )
    
    # Add global context to environment
    env.globals.update(create_global_context())
    
    # Render the detected files
    for template_name, output_filename in configs:

        template = env.get_template(template_name)

        content = template.render()
        with open(docs_dir / output_filename, "w") as f:
            f.write(content)

def main():
    """Generate all BQL documentation files and build HTML output.
    
    Orchestrates the generation of:
    - Complete function reference (bql_functions.rst)
    - Category-specific function summaries (functions_<category>.rst)
    - Aggregate functions summary (functions_aggregates.rst)
    - Table column documentation (columns_*.rst)
    - Category index (functions.rst)
    - Final HTML output via Sphinx
    """
    script_dir = Path(__file__).parent.parent

    # Setup output directory
    docs_dir = script_dir / "build" / "rst"
    docs_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy documentation assets (logos, sphinx configuration, ...) 
    # to output directory
    source_docs = script_dir / "docs"
    for file in source_docs.glob("*"):
        if not file.name.endswith('.j2'):
            shutil.copy(file, docs_dir)
    
    # Create Jinja2 environment with global context
    template_dir = script_dir / "docs"
    
    # Prepare and render all templates (*.rst.j2 -> *.rst)
    render_templates(docs_dir, template_dir)
    
    # Build Sphinx documentation
    html_dir = docs_dir.parent / "html"
    os.system(f"sphinx-build -W {docs_dir} {html_dir}")


if __name__ == "__main__":
    main()
