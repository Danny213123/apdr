"""Apache Solr client using sunburnt (archived package).
sunburnt -> sunburnt on PyPI (must pin old version)."""
import sunburnt
import urllib2

si = sunburnt.SolrInterface("http://localhost:8983/solr/")
response = si.query("test").execute()
