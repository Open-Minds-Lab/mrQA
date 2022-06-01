import jinja2
import weasyprint

def render_html(params):
    """
    Render html page using jinja2
    :param row:
    :return:
    """
    template_loader = jinja2.FileSystemLoader(searchpath="./")
    template_env = jinja2.Environment(loader=template_loader)
    template_file = "layout.html"
    template = template_env.get_template(template_file)
    output_text = template.render(
        name=row.Name
    )

    weasyprint.HTML(string=output_text).write_pdf("report.pdf")
