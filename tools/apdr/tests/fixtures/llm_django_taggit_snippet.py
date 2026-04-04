"""Django app using taggit and taggit_autocomplete.
taggit_autocomplete maps to django-taggit-autosuggest or
django-taggit-autocomplete-modified on PyPI."""
from django.contrib import admin
from django.db import models
from django.template.defaultfilters import slugify
from django.utils.translation import ugettext_lazy as _
from taggit.forms import TagField
from taggit_autocomplete.managers import TaggableManager
from taggit_autocomplete.widgets import TagAutocomplete
