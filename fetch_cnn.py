import sys
import urllib2
import httplib
import urlparse
import time
import codecs
import os.path
import random

from optparse import OptionParser

try:
    import curate
except ImportError:
    curate = False

#disable curating
curate = False

import feedparser
from BeautifulSoup import BeautifulSoup, Comment, Tag

counter = 0

def scrape_url(url):
    page = urllib2.urlopen(url)
    soup = BeautifulSoup(page, convertEntities=BeautifulSoup.ALL_ENTITIES)
    title = scrape_title(soup)
    highlights = scrape_highlights(soup)
    content = scrape_content(soup)
    return title, highlights, content

def scrape_title(soup):
    rc = soup.find('div', id='cnnContentContainer')
    title = rc.h1.string.strip()
    return title
    
def scrape_content(soup):
    txt = soup.find('div', attrs={"class":'cnn_strycntntlft'})

    #remove as much junk from the content part as possible
    [tag.extract() for tag in txt.findAll(attrs={"class" : "cnn_strylctcntr"})]
    [tag.extract() for tag in txt.findAll(attrs={"class" : "cnn_strylftcntnt"})]
    [tag.extract() for tag in txt.findAll(attrs={"class" : "cnnStryVidCont"})]
    [tag.extract() for tag in txt.findAll(attrs={"class" : "cnn_strybtntoolsbttm"})]
    [tag.extract() for tag in txt.findAll(attrs={"class" : "cnn_strybtmcntnt"})]
    
    paragraphs = txt.findAll('p', recursive=False)
    result = []
    for paragraph in paragraphs:
        content = scrape_text(paragraph)
        if content.strip() != "":
            result.append(content.strip())
 
    return result
    
def scrape_highlights(soup):
    rc = soup.find('ul', attrs={"class":'cnn_bulletbin cnnStryHghLght'})
    highlights = rc.findAll(lambda tag: tag.name == 'li' and not tag.has_key('class'))
    return [scrape_text(highlight) for highlight in highlights]

def scrape_text(content):
    #scrapes the text from an element.
    #removes comments
    #removes <tag>NEW:</tag>
    #removes <tag>*(CNN)</tag>
    #removes -- at start of text
    result = []
    for item in content:
        if isinstance(item, Comment):
            continue
        if not item.string:
            continue
        if isinstance(item, Tag):
            if item.string.strip() == "NEW:":
                continue
            if item.string.strip().endswith('(CNN)'):
                continue
        #if all blank space but more than one character
        if item.string.strip() == "" and len(item.string) > 1:
            continue
        if item.string.strip().startswith('--'):
            item.string = item.string.strip()[2:]
        result.append(item.string)
    return "".join(result)

def feed_collect_urls_with_time(feed):
    document = feedparser.parse(feed)
    result = [(resolve_cnn_url(entry.link), entry.updated_parsed) for entry in document.entries]
    return result



    
def build_document(id, title, highlights, content, source):
    result = ['<doc id="%s">' % id]
    result.append('<title>%s</title>' % title)
    result.append('<source>%s</source>' % source)
    result.append('<highlights>')
    result.extend(['<highlight id="%s.%s">%s</highlight>' % (id, i, highlight) for i, highlight in enumerate(highlights)])
    result.append('</highlights>')
    result.append('<text>')
    result.extend(['<paragraph id="%s.%s">%s</paragraph>' % (id, i, paragraph) for i, paragraph in enumerate(content)])
    result.append('</text>')
    result.append('</doc>')
    return "\n".join(result)

def get_updated_urls(latest, store):
    result = []
    for url, date in latest:
        if store.has_key(url):
            old_date = store[url]
            if date <= old_date:
                continue
        result.append(url)
        store[url] = date
    return result

def resolve_url(url):
    parsedurl = urlparse.urlparse(url)
    c = httplib.HTTPConnection(parsedurl.netloc)
    c.request("GET", parsedurl.path)
    r = c.getresponse()
    l = r.getheader('Location')
    if l == None:
        return url # it might be impossible to resolve, so best leave it as is
    else:
        return l
    
def resolve_cnn_url(url):
    l = resolve_url(url)
    pu = urlparse.urlparse(l)
    return urlparse.urlunsplit((pu[0], pu[1], pu[2], '',''))

def generate_id(url):
    pu = urlparse.urlparse(url)
    split = pu.path.split('/')
    did = (split[1], split[-4], split[-3], split[-2])
    return ".".join(did)

def process_url(url, outdir):
    global counter
    did = generate_id(url)
    print "processing", url
    title, highlights, content = scrape_url(url)
    if title == "" or highlights == [] or content == []:
        print "missing data for", url, "so skipping"
        return
    document = build_document(did, title, highlights, content, url)
    print "writing %s/%s" % (outdir, did)
    output = codecs.open("%s/%s" % (outdir, did), "w", "utf8")
    output.write(document)
    output.close()
    if curate:
        print "annotating document"
        curate.pool_requests(curate.curate_text, (([highlights, counter], {}), ([content, counter+1], {})))
    counter += 2

def archive_collect_urls(archive):
    page = urllib2.urlopen(archive)
    soup = BeautifulSoup(page, convertEntities=BeautifulSoup.ALL_ENTITIES)
    headings = soup.findAll(attrs={"class" : "archive-item-headline"})
    urls = []
    pu = urlparse.urlsplit(archive)
    for heading in headings:
        urls.append(urlparse.urlunsplit((pu[0], pu[1], heading.a['href'], '', '')))
    return urls
    
def main(argv):
    usage = "usage: %prog [options] <sources, either feeds, cnn archive urls, cnn story urls, files containing cnn story urls>"
    parser = OptionParser(usage)
    parser.add_option("-d", "--delay", type="int", nargs=1,
                      dest="delay", metavar="<delay in seconds>",
                      help="delay in seconds before checking feeds",
                      default=900)
    parser.add_option("-o", "--output", type="string", nargs=1,
                      dest="output", metavar="<output directory>",
                      help="output directory",
                      default="cnn")
    parser.add_option("-a", "--archive", action="store_true",
                      dest="archive", metavar="download archives",
                      help="download from cnn archives")
    parser.add_option("-u", "--urls", action="store_true",
                      dest="url", metavar="download from urls",
                      help="download from urls directly")

    (options, args) = parser.parse_args(argv)
    outdir = options.output
    delay = options.delay
    fetch_archive = options.archive
    fetch_urls = options.url

    feeds = ['http://rss.cnn.com/rss/cnn_latest.rss', 'http://rss.cnn.com/rss/cnn_topstories.rss', 'http://rss.cnn.com/rss/cnn_world.rss', 'http://rss.cnn.com/rss/cnn_us.rss', 'http://rss.cnn.com/rss/cnn_allpolitics.rss', 'http://rss.cnn.com/rss/cnn_crime.rss', 'http://rss.cnn.com/rss/cnn_tech.rss', 'http://rss.cnn.com/rss/cnn_health.rss']

    archives = ['http://www.cnn.com/WORLD/europe/archive/', 'http://www.cnn.com/WORLD/asiapcf/archive/', 'http://www.cnn.com/WORLD/meast/archive/', 'http://www.cnn.com/WORLD/americas/archive/', 'http://www.cnn.com/WORLD/africa/archive/', 'http://www.cnn.com/US/archive/', 'http://www.cnn.com/POLITICS/archive/', 'http://www.cnn.com/CRIME/archive/', 'http://www.cnn.com/HEALTH/archive/', 'http://www.cnn.com/TECH/archive/']
    
    sources = []
    if len(args) != 0:
        files = []
        for arg in args:
            if os.path.exists(arg):
                f = codecs.open(arg, "r", "utf8")
                contents = f.read()
                f.close()
                sources.extend(s.strip() for s in contents.split() if s.strip() != "")
            else:
                sources.append(arg)
    
    if sources == []:
        if fetch_archive:
            sources = archives
        elif not fetch_urls:
            sources = feeds

    store = {}
    
    if fetch_archive:
        tc = 0
        pc = 0
        sc = 0
        for archive in sources:
            allurls = archive_collect_urls(archive)
            urls = []
            for url in allurls:
                try:
                    did = generate_id(url)
                except Exception as e:
                    print e
                    continue
                if url not in store and not os.path.exists("%s/%s" % (outdir, did)):
                    urls.append(url)
                store[url] = True
            print 'queried archive', archive, 'found', len(allurls), 'urls of which', len(urls), 'required downloading'
            tc += len(allurls)
            pc += len(urls)
            for url in urls:
                try:
                    process_url(url, outdir)
                except Exception as e:
                    print e.args
                    print "skipping"
                    sc += 1
                time.sleep(0.02)
        print "found %d urls, attempted to process %d, skipped %d" % (tc, pc, sc)
        sys.exit(0)
    elif fetch_urls:
        tc = 0
        pc = 0
        sc = 0
        urls = []
        for url in sources:
            did = generate_id(url)
            if url not in store and not os.path.exists("%s/%s" % (outdir, did)):
                urls.append(url)
            store[url] = True
        print 'found', len(sources), 'urls of which', len(urls), 'required downloading'
        tc += len(sources)
        pc += len(urls)
        for url in urls:
            try:
                process_url(url, outdir)
            except Exception, e:
                print e.args
                print "skipping"
                sc += 1
            time.sleep(0.02)
        print "found %d urls, attempted to process %d, skipped %d" % (tc, pc, sc)
        sys.exit(0)
            
    while True:
        tc = 0
        pc = 0
        sc = 0
        for feed in sources:
            latest = feed_collect_urls_with_time(feed)
            tc += len(latest)
            urls = get_updated_urls(latest, store)
            print 'queried feed', feed, 'found', len(latest), 'urls of which', len(urls), 'required update'
            pc += len(urls)
            for url in urls:
                try:
                    process_url(url, outdir)
                except Exception, e:
                    print "Exception: %s > %s" % (type(e), e)
                    print "skipping"
                    sc += 1
                    continue
        print "found %d urls, attempted to process %d, skipped %d" % (tc, pc, sc)
        print "sleeping for", delay, "seconds"
        time.sleep(delay)
    
if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
