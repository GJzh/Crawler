import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
#from lxml import html,etree
import re, os
from time import time
# for extract_next_links and is_valid by Zhen Chen and Jun Guo
import urllib2
from bs4 import BeautifulSoup
import requests

try:
    # For python 2
    from urlparse import urlparse, parse_qs, urljoin
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = (set() 
    if not os.path.exists("successful_urls.txt") else 
    set([line.strip() for line in open("successful_urls.txt").readlines() if line.strip() != ""]))
MAX_LINKS_TO_DOWNLOAD = 500

# written by Zhen Chen and Jun Guo
MOL_Pages = set() # pages with the most out links
MOL_number = 0 # most out links
Subdomains_visited = dict() # subdomains visited
Invalid_links_number = 0 # invalid links it received from the frontier
MT_Pages = set() # pages with the most invalid links
MT_number = 0 # number of invalid links in the MT_pages
TRAP_pool = {'calendar'}


@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "85282960_83322637"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        self.UserAgentString = "R W17 Grad 85282960, 83322637"
		
        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        global Invalid_links_number

        for g in self.frame.get_new(OneUnProcessedGroup):
            print "Got a Group"
            count_invalid_links(g)
            outputLinks, urlResps = process_url_group(g, self.UserAgentString)
            for urlResp in urlResps:
                if urlResp.bad_url and self.UserAgentString not in set(urlResp.dataframe_obj.bad_url):
                    urlResp.dataframe_obj.bad_url += [self.UserAgentString]
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
        if len(url_count) >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def shutdown(self):
        runtime = time() - self.starttime
        print "downloaded ", len(url_count), " in ", runtime, " seconds."
        try:
            download_time = runtime / len(url_count)
        except ZeroDivisionError:
            download_time = -1
        print_results(download_time)
        pass

def save_count(urls):
    global url_count
    urls = set(urls).difference(url_count)
    url_count.update(urls)
    if len(urls):
        with open("successful_urls.txt", "a") as surls:
            surls.write(("\n".join(urls) + "\n").encode("utf-8"))

def process_url_group(group, useragentstr):
    #print 'here'
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    #print 'there'
    save_count(successfull_urls)
    return extract_next_links(rawDatas), rawDatas
    
#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''

def count_invalid_links(g):
    global Invalid_links_number

    for link in g.link_group:
        if not is_valid(link.full_url):
            Invalid_links_number += 1

def print_results(download_time):
    global MOL_Pages, MOL_number
    global Subdomains_visited
    global Invalid_links_number
    global MT_Pages, MT_number

    with open('analytics results.txt', 'w') as outfile:
        print>>outfile, '1. Subdomains visited:'
        for sd in Subdomains_visited:
            print>>outfile, '{0} different URLs from {1}.'.format(Subdomains_visited[sd], sd)
        print>>outfile, '\n2. Number of invalid links received from the frontier: {0}.'.format(Invalid_links_number)
        print>>outfile, '\n3. Pages with the most out links:'
        for page in MOL_Pages:
            print>>outfile, page
        print>>outfile, 'The number of out links is: {0}.'.format(MOL_number)
        print>>outfile, '\n4. Average download time per URL time: {0} seconds'.format(download_time)
        print>>outfile, '\n5. Pages with the most trap links:'
        for page in MT_Pages:
            print>>outfile, page
        print>>outfile, 'The number of trap links is: {0}.'.format(MT_number)


def extract_next_links(rawDatas):
    outputLinks = list()
    '''
    rawDatas is a list of objs -> [raw_content_obj1, raw_content_obj2, ....]
    Each obj is of type UrlResponse  declared at L28-42 datamodel/search/datamodel.py
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''
    global MOL_Pages, MOL_number
    global Subdomains_visited
    global MT_Pages, MT_number

    for urlResp in rawDatas:
        #print urlResp
        #continue
        if urlResp.error_message == '' and not urlResp.bad_url and urlResp.http_code < 400:
            content = urlResp.content.decode('utf-8')
            #content = urlResp.content
            
            if urlResp.is_redirected:
                baseurl = urlResp.final_url
            else:
                baseurl = urlResp.url
            soup = BeautifulSoup(content, 'lxml')

            out_links_count = 0
            mt_num = 0
            for link in soup.find_all('a'):
                if 'href' in link.attrs:
                    newurl = link.attrs['href']
                    # relative url changes to absolution url
                    if not newurl.startswith('http') and not newurl.startswith('ftp'):
                        newurl = urljoin(baseurl, newurl)
                    # ignore the url which not starts with http ands https
                    if newurl.startswith('http'):
                        urlResp.out_links.add(newurl)
                        outputLinks.append(newurl)
                        if not is_valid(newurl):
                            mt_num += 1
            out_links_count = len(urlResp.out_links)
            
            # update the pages with the most out links
            if len(MOL_Pages) != 0:
                if MOL_number == out_links_count:
                    MOL_Pages.add(baseurl)
                elif MOL_number < out_links_count:
                    MOL_Pages.clear()
                    MOL_Pages.add(baseurl)
                    MOL_number = out_links_count
            else:
                MOL_Pages.add(baseurl)
                MOL_number = out_links_count

            # update the pages with the most trap links
            if len(MT_Pages) != 0:
                if MT_number == mt_num:
                    MT_Pages.add(baseurl)
                elif MT_number < mt_num:
                    MT_Pages.clear()
                    MT_Pages.add(baseurl)
                    MT_number = mt_num
            else:
                MOL_Pages.add(baseurl)
                MOL_number = out_links_count


            # count the subdomains and the URL from them
            parsed = urlparse(baseurl)
            subdomain = parsed.hostname
            Subdomains_visited[subdomain] = Subdomains_visited.get(subdomain,0) + 1 # the total out links maybe with some invalid links

    '''
    with open('outlinks.txt','w') as o:
        for l in outputLinks:
            print>>o, l
    '''
    print outputLinks
    return outputLinks # the total output links maybe with some invalid links

def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
    parsed = urlparse(url)
    # http status code should smaller than 400
    #url_head = requests.head(url)
    #if url_head.status_code >= 400:
    #    return False

    # trap pool
    for trap in TRAP_pool:
        if trap in parsed.path and not parsed.query == '':
            return False
    if re.match('C=.{1};O=.{1}$',parsed.query):
        return False


    # Repeating directories
    path_list = parsed.path.split('/')
    path_set = set(path_list)
    if len(path_list) != len(path_set):
        return False
    

    if parsed.scheme not in set(["http", "https"]):
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            # written by ZC and JG
            + "|war|au|apk|db|Z|java|c|pov|bib|shar|r|results|macros|pde|lif|py|txt|htm|pl"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())
            

    except TypeError:
        print ("TypeError for ", parsed)