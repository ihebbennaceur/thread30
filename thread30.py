import sys, os, json, time, threading, io
import pycurl, certifi
import unicodedata, re
import random
from datetime import datetime, timedelta, timezone

import signal
import sys
import os
import psycopg2
import json


DB_CONFIG={"dbname":"newdb",
           "user":"new_user",
           "password":"iheb",
           "host":"localhost",
           "port":5432}


def handle_exit(signal, frame):
    print("\n[INFO] Gracefully shutting down... Saving progress if needed.")
    sys.exit(0)

# Attach signal handler
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

#
if os.name == 'nt': #windows
  import msvcrt
else: #posix
  import termios, atexit
  from select import select
#
#importlib
#importlib.import_module("../mod.py"))




def clock_ms() -> float:
  return time.monotonic() * 1000.

def nowutc():
  return datetime.now(timezone.utc)

def sleep_mcs(dt_mcs):
  threading.Event().wait(dt_mcs / 1.e6)

# Loads text or binary file.
# NOTE Use load_file(fnp_src, False).decode('utf-8-sig') in order to safe loading UTF-8 files 
#   (or else, under Windows, UTF-8 file may be read e.g. as ANSI, and this causes error). 
def load_file(fnp_src, b_as_text: bool = True):
  access = "rb"
  if b_as_text: access = "r"
  _f = open(fnp_src, access)
  s = _f.read()
  _f.close()
  return s
    
def get_file_length(fnp):
  try:
    return os.path.getsize(fnp)
  except: return -1



def prep_filename(value, allow_unicode=False):
  """
  Modifed version of code in
  https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename
  originally taken from
  https://github.com/django/django/blob/master/django/utils/text.py
  """
  value = str(value)
  if allow_unicode:
    value = unicodedata.normalize('NFKC', value)
  else:
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
  value = re.sub(r'[^\w\s.-]', '', value.lower())
  value = re.sub(r'[-\s]+', '-', value)
  value = re.sub(r'[._]+', '_', value)
  return value # .strip('-_')

#print(prep_filename(",.!@#$%^&*(_-_---___.\\__"))

# Extracts the first match of ptn in str.
# If ptn contains groups (...), returns the 1st group, otherwise whole match.
# If not match, returns an empty string.
def re_extract1(src:str, ptn:str, b_multiline: int = 0):
  if b_multiline:
    x = re.search(ptn, src, re.MULTILINE)
  else:
    x = re.search(ptn, src)
  if x is not None:
    if len(x.groups()) > 0:
      return x.groups()[0]
    return x.string[x.start():x.end()]
  return ""

# Returns a string, containing the last part of obj class name.
def get_strtype9(obj):
  s = str(type(obj))
  if s.find(".") >= 0: return re.sub("^.*[.](.+)['].*$", "\\1", s)
  return re.sub("^[^']*['](.+)['].*$", "\\1", s)

#Converts s to string (if not string yet), than to integer.
# If anything fails, returns vdflt.
def conv_str_to_int(s, vdflt):
    try: return int(str(s))
    except: return vdflt

def conv_ensure_float_int_as_int(x):
  if isinstance(x, float) and x.is_integer():
    return int(x)
  return x
    
# Returns leftmost portion of s, which, being converted into UTF-8, will have length <= nmax
def conv_str_to_limited_len_as_utf8(s, nmax):
  n = 0
  s2 = ""
  for c in s:
    n += len(c.encode("utf8"))
    if n > nmax: break
    s2 += c
  return s2

# Returns h if it's a dict, otherwise {}.
def dict_ensure(h):
  if not isinstance(h, dict):  h = {}
  return h

# Ensures h being dict, and key being in h.
# If key is not in h, sets h[key] = value_dflt.
# If b_set_vdflt_if_type_different == True: if key is in h but h[key] type is different from that of value_dflt, sets h[key] = value_dflt.
# Returns the resulting h (which may differ from the original).
def dict_ensure_key(h, key, value_dflt, b_set_vdflt_if_type_different: bool = False):
  if not isinstance(h, dict):  h = {}
  if not (key in h) or (b_set_vdflt_if_type_different and type(h[key]) != type(value_dflt)): h[key]  = value_dflt
  return h

# Sets terminal value at the end of given path in a dict.
# 1) h is ensured to be dict.
# 2) If path is not empty, path is ensured to exist in h, nested dicts are created automatically.
# 3) If path is not empty, the end value of the path is set to value.
# Returns 
#   a) the resulting h (which may differ from the original).
#   b) b_ret_value == True: returns actual terminal value (existing or assigned)
def dict_ensure_path_value(h, path: str, value, pathsep: str = '.', b_ret_value: bool = False):
  h0 = dict_ensure(h)

  akeys = []
  if type(path) == list: akeys = path
  elif type(path) == str: akeys = path.split(pathsep)
  if len(akeys) == 0: return h0

  h = h0
  for i in range(0, len(akeys)):
    k = akeys[i]
    if i == len(akeys) - 1:
      h[k] = value
    else:
      dict_ensure_key(h, k, {}, True)
      h = h[k]
  if b_ret_value: return h[k]
  return h0

# Gets terminal value at the end of given path in a dict.
# path:
#   a) <path elem.><pathsep><path elem.>...
#   b) list of <path elem.>
# b_readonly = True:
#     a) if vdflt is None, non-existent path generates an exception.
#     b) if vdflt != None, for non-existent path, vdflt is returned.
# b_readonly = False:
#     0) If h is not a dict, no changes are made, and vdflt is returned
#     1) Otherwise, path in h is ensured to exist, nested dicts are created automatically.
#     2) If end value exists, it's returned as is.
#     3) Otherwise, vdflt is set as the end value, and returned as such.
def dict_path(h, path, vdflt = None, b_readonly: bool = True, pathsep: str = '.'):
  if b_readonly:
    if not isinstance(h, dict):  
      if vdflt is not None: return vdflt
      return h[""] # intentionally generates an exception

    akeys = []
    if type(path) == list: akeys = path
    elif type(path) == str: akeys = path.split(pathsep)
    if len(akeys) == 0: return vdflt
    
    for k in akeys:
      if type(h) != dict and vdflt is not None: return vdflt
      if not (k in h): 
        if vdflt is not None: return vdflt
        return h[k] # intentionally generates an exception
      h = h[k]
    return h
  else:
    if not isinstance(h, dict): return vdflt

    akeys = []
    if type(path) == list: akeys = path
    elif type(path) == str: akeys = path.split(pathsep)
    if len(akeys) == 0: return vdflt

    for i in range(0, len(akeys)):
      k = akeys[i]
      if i == len(akeys) - 1:
        if k in h: return h[k]
        h[k] = vdflt
        return vdflt
      else:
        dict_ensure_key(h, k, {}, True)
        h = h[k]

def dict_move_key_to_end(h, k):
  if isinstance(h, dict) and k in h:
    v = h[k]
    h.pop(k)
    h[k] = v

# Decodes JSON string to dict. 
#   If decoding fails for any reason, or the decoded value is not dict, returns dflt.
def dict_from_json_str(s, dflt = {}):
  if type(s) != str or s == "": return dflt
  try:
    h = json.loads(s)
    if type(h) != dict: return dflt
    return h
  except: pass
  return dflt

def dict_merge(h_dest, h_src):
  for key, value in h_src.items():
    if isinstance(value, dict):
      # get node or create one
      node = h_dest.setdefault(key, {})
      dict_merge(node, value)
    else:
      h_dest[key] = value
  return h_dest

# If x is None, return vreplace.
#   Otherwise, returns x.
def replace_if_none(x, vreplace):
  if x is None: return vreplace
  return x



  # Cleans up lines, starting with "//" after whitespaces.
def txt_remove_comments(s):
  return re.sub("^\\s*//.*$", "", s, flags = re.MULTILINE)

# h[k_osdep] entry specifies a branch, containing os-dependent replacements
#   for values in other branches of h.
def _dict_osdep_substitute(h, k_osdep: str = "__osdep"):
  if type(h) != dict : return h
  
  if os.name == 'nt': #windows
    k_os = "windows"
  else: #posix
    k_os = "linux"
  h2 = h[k_osdep][k_os]
  h2b = h[k_osdep]["env2"]
  a3 = h[k_osdep]["env3"]

  #Merge h2b into h2
  for k in h2b.keys():
    h2[k] = h2b[k]

  #For each string value in h2, recursively replace all occurrences of string keys from h2 (except for self-referencing keys) 
  #   with their values (converted to string if necessary).
  while True:
    b_subst1 = False
    for k in h2.keys():
      v = h2[k]
      if type(v) == str:
        sv = str(v)
        b_subst2 = False
        for k2 in h2.keys():
          if k2 == k: continue
          sk2 = str(k2)
          if sv.find(sk2) >= 0:
            sv = sv.replace(sk2, str(h2[k2]))
            b_subst2 = True
        if b_subst2:
          h2[k] = sv
        b_subst1 = b_subst1 or b_subst2
    if not b_subst1: break

  #For each value in a3, representing branch path in h, 
  #   treat appropriate terminal value q = dict_path(a3[i]) as key in h2: replace q with h2[q].
  # All paths must exist in h, and all referenced q must exist in h2 - 
  #   this is responsibility of the user.
  for path in a3:
    if path == k_osdep: continue
    dict_ensure_path_value(h, path, h2[dict_path(h, path)])
    
  return h


# Load configuration file, threat it as JSON, remove comments,
#   decode (into dict), replace certain values with os-dependent values.
#   See also 
def load_cfg(fnp_src):
  if fnp_src == "": fnp_src = os.path.basename(__file__) + ".cfg"
  s = load_file(fnp_src, False).decode('utf-8-sig')
  s = txt_remove_comments(s)
  h = json.loads(s)
  h = _dict_osdep_substitute(h)
  return h

def rmv_empty_subdirs(rootdir: str):
  while True:
    b = 0
    for xdir, xsubdirs, xfiles in list(os.walk(rootdir))[1:]:
      if len(xsubdirs) + len(xfiles) == 0: 
        os.rmdir(xdir)
        b = 1
    if b == 0: break


class kb_getch_nonblk:
  def __init__(self):
    if os.name == 'nt':
      pass
    else:
      self.fd = sys.stdin.fileno()
      self.trm_prv = None
      try:
        trm = termios.tcgetattr(self.fd)
        self.trm_prv = termios.tcgetattr(self.fd)
        trm[3] = (trm[3] & ~termios.ICANON & ~termios.ECHO)
        termios.tcsetattr(self.fd, termios.TCSAFLUSH, trm)
        atexit.register(self.set_normal_term)
      except Exception as e:
        print("kb_getch_nonblk.__init__:", e)

  def set_normal_term(self):
    if os.name == "nt":
      pass
    else:
      if self.trm_prv is not None:
        termios.tcsetattr(self.fd, termios.TCSAFLUSH, self.trm_prv) 
      
  def __del__(self):
    self.set_normal_term()

  def getch(self):
    if os.name == 'nt':
      if not msvcrt.kbhit(): return 0
      c = ord(msvcrt.getch()) # variant: .decode('utf-8')
      return c
    else:
      if self.trm_prv is None:
        return 0
      dr,dw,de = select([sys.stdin], [], [], 0)
      if dr == []: return 0
      c = ord(sys.stdin.read(1)) # variant: .decode('utf-8')
      return c


class th_call_method_async(threading.Thread):
  # obj_'s method may be specified a) by name (as string), or b) as such (e.g. my_thread.run)
  def __init__(self, obj_, method_, aargs_, resdflt_):
    threading.Thread.__init__(self)
    self.obj = obj_
    self.method = method_
    self.aargs = aargs_
    self.result = resdflt_
  def run(self):
    if type(self.method) == str:
      self.result = getattr(self.obj, self.method)(*tuple(self.aargs))
    else:
      self.result = self.method(*tuple(self.aargs))



class wrapper_curl_request:
  class _util_curl_store_str:
    def __init__(self): self.s = ''
    def store(self, buf): self.s = self.s + str(buf, 'utf8')  
    def write(self, buf): self.s = self.s + str(buf, 'utf8')  

  # Sequentially generates pairs [<IP address>, <port>], from the given range.
  class hlp_select_bind_addr_seq:
    # a_binda = 
    #   a) list of [ "host!<host IP>", <base port>, <number of ports to try, starting from base port> ]
    #     It's recommended to use one same value of <number of ports to try> for all addresses.
    #   b) None
    def __init__(self, a_binda = None):
      self._aap = None
      self._ind = 0
      self._lock = threading.Lock()
      if type(a_binda) != list: return
      aap = []
      for a in a_binda:
        if not (type(a) == list and len(a) == 3): continue
        aap.append([a[0], max(0, a[1]), max(1, int(a[2])), 0])
      if len(aap) > 0: self._aap = aap
      return
    
    def n_addrs(self):
      if self._aap is None: return 0
      return len(self._aap)
    
    # This function may be called asynchronously.
    # Returns:
    #   a) next [<IP>, <port>] to use as bind address.
    #     On successive calls, loops through all available IP addresses, each time increasing port number,
    #     associated with each address.
    #   b) None (do not use explicit binding)
    def next_addr(self):
      if self._aap is None: return None
      with self._lock:
        adef = self._aap[self._ind]
        addr = adef[0]
        port = adef[1] + adef[3]
        adef[3] = (adef[3] + 1) % adef[2]
        self._ind = (self._ind + 1) % len(self._aap)
      return [addr, port]
    
  # get_mode: 
  #   0 - GET request (return string), 
  #   1 - HEAD request (return string),
  #   2 - GET request (return bytearray).
  # See also get_resp().
  def __init__(self, src_url_: str, get_mode: int):

    # Parameters, tunable between __init__ and periodic()
    #
    self.tmo_select_s = 1 # default file handle select() timeout, s
    self.tmo_connect_s = 10 # default HTTP GET or HEAD request timeout, s
    self.tmo_transfer_s = -1 # default HTTP body transfer timeout, s (no timeout by default)
    self.is_verbose = 0
      # bind_addr, if set BEFORE 1st CALL to periodic(), must be list (or tuple) of 3 elements:
      # [ "host!<host IP>", <base port>, <number of ports to try, starting from base port> ]
      # That parameters are directly set on the 1st periodic() call to CURL options 
      #     CURLOPT_INTERFACE, CURLOPT_LOCALPORT, CURLOPT_LOCALPORTRANGE
      # NOTE CURL factual behavior is trying always only 1st port number: <base port> + 0, 
      #   and ignoring all other ports: <base port> + [1.. <number of ports to try>).
    self.bind_addr = None
    #
    #------------------------------

    self.src_url = src_url_   
    self._curl = None
    self._mcurl = None
    self._in_perform = 1
    self._num_handles = 1
    self._is_end = 0
    self._is_1st_call = 1
    self._resp_code = -1
    self._errstr = ""

    curl = pycurl.Curl()
    curl.setopt(curl.URL, src_url_)
    if re.match("^\\s*https", src_url_) is not None: curl.setopt(pycurl.CAINFO, certifi.where())
   
    self._hh = None
    self.get_mode = get_mode
    if get_mode == 1:
      curl.setopt(curl.NOBODY, 1)     
      self._hh = self._util_curl_store_str()
      curl.setopt(curl.HEADERFUNCTION, self._hh.store)        
    elif get_mode == 2:
      curl.setopt(curl.NOBODY, 0)     
      self._hh = io.BytesIO()
      curl.setopt(pycurl.WRITEDATA, self._hh) 
    else:
      curl.setopt(curl.NOBODY, 0)     
      self.get_mode = 0
      self._hh = self._util_curl_store_str()
      curl.setopt(pycurl.WRITEDATA, self._hh) 

    mcurl = pycurl.CurlMulti()
    mcurl.add_handle(curl)
    self._curl = curl
    self._mcurl = mcurl
  
  def __del__(self):
    if self._mcurl is not None: self._mcurl.close()
    if self._curl is not None: self._curl.close()
    
  # Actual request normally starts at 1st call to periodic().
  # Returns 1 when finished, 0 until that.
  def periodic(self):
    if self._is_1st_call:
      if self._curl is None or self._mcurl is None:
        self._is_1st_call = 0
        self._is_end = 1
        return
      if self.is_verbose:
        self._curl.setopt(pycurl.VERBOSE, 1)           
      if self.tmo_connect_s >= 0:
        self._curl.setopt(pycurl.CONNECTTIMEOUT, self.tmo_connect_s)
        self._curl.setopt(pycurl.LOW_SPEED_LIMIT, 1)
        self._curl.setopt(pycurl.LOW_SPEED_TIME, 2 * self.tmo_connect_s)
      if self.tmo_transfer_s >= 0:
        self._curl.setopt(pycurl.TIMEOUT, self.tmo_transfer_s)
      if self.bind_addr:
        self._curl.setopt(pycurl.INTERFACE, self.bind_addr[0])
        self._curl.setopt(pycurl.LOCALPORT, self.bind_addr[1])
        self._curl.setopt(pycurl.LOCALPORTRANGE, self.bind_addr[2])
        self._curl.setopt(pycurl.FORBID_REUSE, 1)
      self._is_1st_call = 0
    
    if self._is_end: return 1
    if self._num_handles == 0: 
      self._mcurl.remove_handle(self._curl)
      self._mcurl.close()
      self._is_end = 1
      self._resp_code = self._curl.getinfo(self._curl.RESPONSE_CODE)
      self._errstr = self._curl.errstr()
      return 1
    if self._in_perform:
      ret, self._num_handles = self._mcurl.perform()
      if ret != pycurl.E_CALL_MULTI_PERFORM: self._in_perform = 0
    else:
      ret = self._mcurl.select(self.tmo_select_s)
      if ret != -1: self._in_perform = 1
    return 0
  
  # Depending on self.get_mode: 
  #   0 - GET request (returns string), 
  #   1 - HEAD request (returns string),
  #   2 - GET request (returns bytearray = io.BytesIO.getvalue()).
  def get_resp(self):
    if self._hh is None: return ""
    if isinstance(self._hh, io.BytesIO): return self._hh.getvalue()
    return self._hh.s # expected: self._hh being _util_curl_store_str

  # If .get_resp_code() == 0, see .get_errstr().
  # If .get_resp_code() > 0, see .get_resp() itself.
  def get_resp_code(self):
    return self._resp_code

  def get_errstr(self):
    return self._errstr

  # This should be explicitly called only in case if explicit bind address:port has been set,
  #   and it needs to be immediately reused 
  #   by another instance of wrapper_curl_request.
  def close_curl_handle(self):
    if self._curl: self._curl.close()

    
# Performs multiple curl requests, maybe from several bind addresses.
class curl_batch_job:
  def __init__(self):
    self._nrq_per_binda = 1
    self._hrequests_waiting = {}
    self._hbindap_free = {}
    self._dtpause_s = 0
    self.arequests = []

  # arq_init_[i]: [src_url_: str, get_mode: int], see also wrapper_curl_request.__init__
  # nrq_per_binda_: >= 1, number of simultaneous CURL requests for particular bind address.
  #   If bind addresses are given, in each of them, number of ports that can be tried, must be >= nrq_per_binda.
  # bind_addrs_: optional list of [ "host!<host IP>", <base port>, <number of ports to try, starting from base port> ],
  #   See also wrapper_curl_request.bind_addr.
  def set_jobs(self, arq_init_, nrq_per_binda_: int = 1, bind_addrs_ = None, pause_after_job_s_: float = 0):
    self._nrq_per_binda = max(1, nrq_per_binda_)
    self._hrequests_waiting = {}
    self._hbindap_free = {}
    self._dtpause_s = max(0, pause_after_job_s_)
    self.arequests = [wrapper_curl_request(x[0], x[1]) for x in arq_init_]      
    
    t_initial = clock_ms() - 1000 * (1 + self._dtpause_s) # for this addr:port, set the initial "time last used" to value causing next reuse w/o pause
    if bind_addrs_:
      #NOTE This original (commented) scheme implied that CURL will automatically switch bind port 
      #   on connect retry attempts, using the specified range: x = bind_addrs_[i], range(x[1], x[1]+x[2]).
      #   But, CURL factual behavior is using only 1st port number (x[1]), 
      #   and ignoring "number of ports to try" (x[2]).
      #for i in range(len(bind_addrs_)):
        #self._hbindap_free[i] = (bind_addrs_[i], nowutc() - timedelta(seconds = 1 + self._dtpause_s))
      # Due to the above, each particular bind addr:port descpription is created as individual object.
      i: int = 0
      for ba in bind_addrs_:
        inds = list(range(ba[2]))
        random.shuffle(inds)
        for j in inds:
          port = ba[1] + j
          self._hbindap_free[i] = ([ba[0], port, 1], t_initial) # 1 = try only the specified port
          i += 1
    else:
      self._hbindap_free[0] = (None, t_initial)
          
    self._hbindap_busy = {}
    for irequest in range(len(arq_init_)):
      self._hrequests_waiting[irequest] = 0
      
  #returns kbinda of the address that is available now (for reuse after pause), or else None
  def _find_free_binda(self):
    h = {} # will hold: { <known bind address>, <number of busy ports
    for k in self._hbindap_busy.keys():
      binda, _ = self._hbindap_busy[k]
      a = None if binda is None else binda[1]
      if a not in h: h[a] = 0
      h[a] = h[a] + 1
    for k in self._hbindap_free.keys():
      binda, _ = self._hbindap_free[k]
      a = None if binda is None else binda[1]
      if a not in h: h[a] = 0

    for k in self._hbindap_free.keys():
      binda, t_bindap_last_used = self._hbindap_free[k]
      a = None if binda is None else binda[1]
      if h[a] < self._nrq_per_binda and (clock_ms() - t_bindap_last_used) >= 1000 * self._dtpause_s:
        return k
      
    return None
      
  # ~!!! describe actual alg.
  # Actual request normally starts at 1st call to periodic().
  # Returns:
  # 1 when all jobs are finished.
  #   self.arequests contains list of wrapper_curl_request, corresponding to arq_init_ arg. of set_jobs
  # 0 if any job is pending.
  def periodic(self):
    if len(self._hrequests_waiting) > 0 and len(self._hbindap_free) > 0:
      kbinda = self._find_free_binda()
      if kbinda is not None:
        irequest = -1
        for k in self._hrequests_waiting.keys():
          irequest = k
          break
        if irequest >= 0:
          binda, _ = self._hbindap_free[kbinda]
          request = self.arequests[irequest]
          request.bind_addr = binda
          #print("__reusing__", request.bind_addr, "for", request.src_url)
          request.periodic() # the 1st call for this request
          self._hbindap_busy[kbinda] = (binda, irequest)
          self._hbindap_free.pop(kbinda)
          self._hrequests_waiting.pop(irequest)

    b_busy = 0
    for kbinda in list(self._hbindap_busy.keys()):
      binda, irequest = self._hbindap_busy[kbinda]
      request = self.arequests[irequest]
      if request.periodic() == 0: 
        b_busy = 1
      else:
        request.close_curl_handle()
        self._hbindap_free[kbinda] = (binda, clock_ms())
        self._hbindap_busy.pop(kbinda)
        
    if b_busy or len(self._hrequests_waiting) > 0:
      return 0
    
    return 1

def curl_err_is_bind_or_resolve_or_tmo(s_errstr: str):
  return re_extract1(s_errstr, "Address already in use") != "" \
    or re_extract1(s_errstr, "Could not resolve.+Successful completion") != "" \
    or re_extract1(s_errstr, "Connection timeout after.+ms") != ""

def curl_err_is_timeout(s_errstr: str):
  return re_extract1(s_errstr, "timed out after") != ""
import os
import json
import re
import time
import threading
import psycopg2
from typing import List, Dict, Tuple

# Configuration
MAX_DOWNLOAD_ATTEMPTS = 3
BASE_DIR = os.path.join(os.getcwd(), "/home/iheb/Desktop/img")
IMAGE_EXTENSIONS_REGEX = re.compile(r"(?i)(\.jpg|\.jpeg|\.png|\.gif)")

MAX_PRODUCTS_PER_ITER = 3
SUCCESS_THRESHOLD = 0.99  # 60% success rate

# Database Connection
conn = psycopg2.connect(
   **DB_CONFIG
)
cursor = conn.cursor()

# Threading lock for safe JSON writes
json_lock = threading.Lock()

def create_image_path(img_url: str, product_id: str, index: int, category: str) -> str:
    match = IMAGE_EXTENSIONS_REGEX.search(img_url)
    ext = match.group(0) if match else ".jpg"
    filename = f"{product_id}_{index}{ext}"
    return os.path.join(BASE_DIR, product_id, category, filename)
def insert_product(product_id: str, product_details: Dict, conn):
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO mproducts (product_id, product_details, status)
                VALUES (%s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
                """,
                (product_id, json.dumps(product_details), "pending")
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to insert product {product_id}: {e}")

# Function to insert images into database
def insert_images(product_id: str, category: str, success_images: List[Tuple[str, str]], conn):
    try:
        with conn.cursor() as cur:
            for original_url, new_path in success_images:
                cur.execute(
                    """
                    INSERT INTO mimages (product_id, original_url, category, new_location)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (product_id, original_url, category, new_path)
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to insert images for {product_id}: {e}")

def download_images(image_urls: List[str], product_id: str, category: str) -> Tuple[int, List[Tuple[str, str]], List[int]]:
    image_paths = []
    arequests = []
    airq2iimg = []
    
    product_folder = os.path.join(BASE_DIR, product_id, category)
    os.makedirs(product_folder, exist_ok=True)

    for idx, img_url in enumerate(image_urls):
        file_path = create_image_path(img_url, product_id, idx + 1, category)
        image_paths.append(file_path)
        
        full_url = "https:" + img_url if img_url.startswith("//") else img_url
        arequests.append([full_url, 2])  # Mode 2: GET request (return byte array)
        airq2iimg.append(idx)

    bj = curl_batch_job()
    bj.set_jobs(arequests, 1, None)

    success_count = 0
    success_images = []
    aiimg_err_retry = []

    while True:
        if bj.periodic():
            break
        sleep_mcs(10_000)

    for i in range(len(bj.arequests)):
        iimg = airq2iimg[i]
        img_url = image_urls[iimg]
        file_path = image_paths[iimg]
        rq = bj.arequests[i]

        if rq.get_resp_code() == 200:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(rq.get_resp())
            success_count += 1
            success_images.append((img_url, file_path))
            print(f"[SUCCESS] {img_url} -> {file_path}")

        elif rq.get_resp_code() == 0:
            aiimg_err_retry.append(iimg)

    return success_count, success_images, aiimg_err_retry


def insert_sku(product_id: str, sku_data: List[Dict], conn):
    try:
        with conn.cursor() as cur:
            for sku in sku_data:
                cur.execute(
                    """
                    INSERT INTO msku (sku_id, product_id, sku_props, sku_image, original_price, quantity, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sku_id) DO NOTHING
                    """,
                    (
                        sku.get("skuId"),
                        product_id,
                        json.dumps(sku.get("skuProps", [])),
                        sku["skuImage"].get("RU", ""),
                        sku.get("originalPrice", 0),
                        sku.get("quantity", 0),
                        sku.get("status", "active")
                    )
                )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to insert SKU for {product_id}: {e}")

def process_products_in_batches(json_file: str):
    conn = psycopg2.connect(**DB_CONFIG)
    with open(json_file, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print("[ERROR] Invalid JSON structure.")
            return
    
    if isinstance(data, list): 
        products = data
    elif isinstance(data, dict):
        if "productId" in data:
            products = [data]
        else:
            products = [value for value in data.values() if isinstance(value, dict)]
    else:
        print("[ERROR] JSON data must be a list or dictionary with a 'productId'.")
        return 

    if not products:
        print("[INFO] No valid product data found in JSON file.")
        return

    while products:
        current_batch = products[:MAX_PRODUCTS_PER_ITER]
        products = products[MAX_PRODUCTS_PER_ITER:]

        for product in current_batch:
            product_id = product.get("productId")
            main_images = product.get("mainImages", {}).get("RU", [])
            desc_images = product.get("descImg", {}).get("RU", [])
            #sku_data = product.get("sku", [])
            sku_images = [sku.get("skuImage", {}).get("RU") for sku in product.get("sku", []) if sku.get("skuImage", {}).get("RU")]

            if not product_id:
                print(f"[ERROR] Missing 'productId' in product: {product}")
                continue

            insert_product(product_id, product, conn)  # Ensure product is inserted before images
            insert_sku(product_id, sku_images, conn)  # Insert SKUs into database

            for category, images in [("main", main_images), ("desc", desc_images), ('sku',sku_images)]:
                if not images:
                    print(f"[INFO] No {category} images found for product {product_id}")
                    continue
                success_count, success_images, _ = download_images(images, product_id, category)
                insert_images(product_id, category, success_images, conn)
    
    conn.close()
    print("[INFO] All products processed")
'''
def process_products_in_batches(json_file: str):
    conn = psycopg2.connect(**DB_CONFIG)
    with open(json_file, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print("[ERROR] Invalid JSON structure.")
            return
    
    if isinstance(data, list): 
        products = data
    elif isinstance(data, dict):
        if "productId" in data:
            products = [data]
        else:
            products = [value for value in data.values() if isinstance(value, dict)]
    else:
        print("[ERROR] JSON data must be a list or dictionary with a 'productId'.")
        return 

    if not products:
        print("[INFO] No valid product data found in JSON file.")
        return

    while products:
        current_batch = products[:MAX_PRODUCTS_PER_ITER]
        products = products[MAX_PRODUCTS_PER_ITER:]

        for product in current_batch:
            product_id = product.get("productId")
            main_images = product.get("mainImages", {}).get("RU", [])
            desc_images = product.get("descImg", {}).get("RU", [])
            sku_images = [sku.get("skuImage", {}).get("RU") for sku in product.get("sku", []) if sku.get("skuImage", {}).get("RU")]

            if not product_id:
                print(f"[ERROR] Missing 'productId' in product: {product}")
                continue

            insert_product(product_id, product, conn)  # Ensure product is inserted before images

            for category, images in [("main", main_images), ("desc", desc_images), ("sku", sku_images)]:
                if not images:
                    print(f"[INFO] No {category} images found for product {product_id}")
                    continue
                success_count, success_images, _ = download_images(images, product_id, category)
                insert_images(product_id, category, success_images, conn)
    
    conn.close()
    print("[INFO] All products processed")
'''
if __name__ == "__main__":
    json_file = "product_data.json"
    process_products_in_batches(json_file)