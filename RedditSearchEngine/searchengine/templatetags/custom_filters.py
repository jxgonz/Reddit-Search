from django import template

register = template.Library()

@register.filter
def truncate_with_ellipsis(value, max_length):
    if len(value) > max_length:
        return value[:max_length-3] + '...'
    else:
        return value