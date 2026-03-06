import tempfile, os, json, datetime, dnf, urllib.request, sys, koji
import re, time, hashlib, gzip
from concurrent.futures import ProcessPoolExecutor, as_completed

import multiprocessing, asyncio
from content_resolver.utils import dump_data, load_data, log, err_log, pkg_id_to_name, size, workload_id_to_conf_id, url_to_id
from content_resolver.exceptions import RepoDownloadError, BuildGroupAnalysisError, KojiRootLogError, AnalysisError


def pkg_placeholder_name_to_id(placeholder_name):
    placeholder_id = "{name}-000-placeholder.placeholder".format(name=placeholder_name)
    return placeholder_id


def pkg_placeholder_name_to_nevr(placeholder_name):
    placeholder_id = "{name}-000-placeholder".format(name=placeholder_name)
    return placeholder_id


#####################################################
### Find build dependencies from a Koji root log ###
####################################################

def _get_build_deps_from_a_root_log(root_log):
    """
    Given a packages Koji root_log, find its build dependencies.
    """
    required_pkgs = []

    # The individual states are nicely described inside the for loop.
    # They're processed in order
    state = 0

    for file_line in root_log.splitlines():

        # 0/
        # parts of the log I don't really care about
        if state == 0:

            if "'builddep', '--installroot'" in file_line:
                state += 1


        # 1/
        # getting the "already installed" packages to the list
        elif state == 1:

            if "is already installed." in file_line:
                parts = file_line.split()
                pkg_name = parts[3].strip('"').rsplit("-",2)[0]
                required_pkgs.append(pkg_name)

            elif "Dependencies resolved." in file_line:
                state += 1

            elif "Repositories loaded." in file_line:
                state += 1


        # 2/
        # going through the log right before the first package name
        elif state == 2:

            if "is already installed." in file_line:
                parts = file_line.split()
                pkg_index = parts.index("already") - 2
                pkg_name = parts[pkg_index].strip('"').rsplit("-",2)[0]
                required_pkgs.append(pkg_name)

            if "Installing:" in file_line:
                parts = file_line.split()
                if len(parts) == 3:
                    state += 1


        # 3/
        # And now just saving the packages until the "installing dependencies" part
        # or the "transaction summary" part if there's no dependencies
        elif state == 3:

            if "Installing dependencies:" in file_line:
                state = 2

            elif "Transaction Summary" in file_line:
                state = 2

            else:
                parts = file_line.split()
                num_parts = len(parts)

                if num_parts >= 3 and parts[2] == "Package" and parts[-1] == "installed.":
                    pkg_name = parts[3].strip('"').rsplit("-",2)[0]
                    required_pkgs.append(pkg_name)

                elif num_parts in (10, 11):
                    pkg_index = parts.index("already") - 2
                    pkg_name = parts[pkg_index].strip('"').rsplit("-",2)[0]
                    required_pkgs.append(pkg_name)
                    if pkg_index == 3:
                        pkg_name = parts[7]
                    else:
                        pkg_name = parts[2]
                    required_pkgs.append(pkg_name)

                elif num_parts in (8, 3):
                    pkg_name = parts[2]
                    required_pkgs.append(pkg_name)

                elif num_parts in (7, 4):
                    continue

                elif num_parts in (6, 5):
                    if parts[4] in ("B", "KiB", "k", "MiB", "M", "GiB", "G"):
                        continue
                    else:
                        pkg_name = parts[2]
                        required_pkgs.append(pkg_name)

                else:
                    raise KojiRootLogError


        # 4/
        # I'm done. So I can break out of the loop.
        elif state == 4:
            break


    return required_pkgs


def _get_koji_log_path(srpm_id, arch, koji_session):
    """
    Get koji log path for a given SRPM.
    """
    MAX_TRIES = 10
    attempts = 0

    while attempts < MAX_TRIES:
        try:
            koji_pkg_data = koji_session.getRPM(f"{srpm_id}.src")
            koji_logs = koji_session.getBuildLogs(koji_pkg_data["build_id"])
            break
        except Exception:
            attempts += 1
            if attempts == MAX_TRIES:
                raise KojiRootLogError("Could not talk to Koji API")
            time.sleep(1)

    koji_log_path = None
    for koji_log in koji_logs:
        if koji_log["name"] == "root.log":
            if koji_log["dir"] == arch or koji_log["dir"] == "noarch":
                koji_log_path = koji_log["path"]
                break

    return koji_log_path


def _download_root_log_with_retry(root_log_url):
    """
    Download root.log file with retry logic.
    """
    MAX_TRIES = 10
    attempts = 0

    request = urllib.request.Request(root_log_url)
    request.add_header("Accept", "text/plain")
    request.add_header("User-Agent", "ContentResolver/1.0")

    while attempts < MAX_TRIES:
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                root_log_data = response.read()
                return root_log_data.decode('utf-8')
        except Exception:
            attempts += 1
            if attempts == MAX_TRIES:
                raise KojiRootLogError(f"Could not download root.log from {root_log_url}")
            time.sleep(1)

def process_single_srpm_root_log(work_item):
    """
    Process a single SRPM's root.log file.

    Args:
        work_item (dict): Contains koji_api_url, koji_files_url, srpm_id, arch, dev_buildroot

    Returns:
        dict: Contains srpm_id, arch, deps (list), error (str or None)
    """
    try:
        koji_api_url = work_item['koji_api_url']
        koji_files_url = work_item['koji_files_url']
        srpm_id = work_item['srpm_id']
        arch = work_item['arch']
        dev_buildroot = work_item.get('dev_buildroot', False)

        # Handle development buildroot mode
        if dev_buildroot:
            # Making sure there are 3 passes at least, but that it won't get overwhelmed
            srpm_name = srpm_id.rsplit("-", 2)[0]
            if srpm_name in ["bash", "make", "unzip"]:
                return {
                    'srpm_id': srpm_id,
                    'arch': arch,
                    'deps': ["gawk", "xz", "findutils"],
                    'error': None
                }
            elif srpm_name in ["gawk", "xz", "findutils"]:
                return {
                    'srpm_id': srpm_id,
                    'arch': arch,
                    'deps': ['cpio', 'diffutils'],
                    'error': None
                }
            return {
                'srpm_id': srpm_id,
                'arch': arch,
                'deps': ["bash", "make", "unzip"],
                'error': None
            }

        # Handle special cases
        if srpm_id.rsplit("-", 2)[0] in ["shim"]:
            return {
                'srpm_id': srpm_id,
                'arch': arch,
                'deps': [],
                'error': None
            }

        # Create koji session
        koji_session = koji.ClientSession(koji_api_url, opts={"timeout": 20})

        # Get koji log path
        koji_log_path = _get_koji_log_path(srpm_id, arch, koji_session)

        if not koji_log_path:
            return {
                'srpm_id': srpm_id,
                'arch': arch,
                'deps': [],
                'error': None
            }

        # Download root.log
        root_log_url = f"{koji_files_url}/{koji_log_path}"
        root_log_contents = _download_root_log_with_retry(root_log_url)

        # Parse dependencies
        deps = _get_build_deps_from_a_root_log(root_log_contents)

        return {
            'srpm_id': srpm_id,
            'arch': arch,
            'deps': deps,
            'error': None
        }

    except Exception as e:
        return {
            'srpm_id': work_item.get('srpm_id', 'unknown'),
            'arch': work_item.get('arch', 'unknown'),
            'deps': [],
            'error': str(e)
        }

class Analyzer():

    ###############################################################################
    ### Analyzing stuff! ##########################################################
    ###############################################################################

    # Configs:
    #   TYPE:           KEY:          ID:
    # - repo            repos         repo_id
    # - env_conf        envs          env_id
    # - workload_conf   workloads     workload_id
    # - label           labels        label_id
    # - conf_view       views         view_id
    #
    # Data:
    #   TYPE:         KEY:                 ID:
    # - pkg           pkgs/repo_id/arch    NEVR
    # - env           envs                 env_id:repo_id:arch_id
    # - workload      workloads            workload_id:env_id:repo_id:arch_id
    # - view          views                view_id:repo_id:arch_id
    #
    # self.tmp_dnf_cachedir is either "dnf_cachedir" in TemporaryDirectory or set by --dnf-cache-dir
    # contents:
    # - "dnf_cachedir-{repo}-{arch}"                     <-- internal DNF cache
    #
    # self.tmp_installroots is "installroots" in TemporaryDirectory
    # contents:
    # - "dnf_generic_installroot-{repo}-{arch}"          <-- installroots for _analyze_pkgs
    # - "dnf_env_installroot-{env_conf}-{repo}-{arch}"   <-- installroots for envs and workloads and buildroots
    #
    # 

    def __init__(self, configs, settings):
        self.workload_queue = {}
        self.workload_queue_counter_total = 0
        self.workload_queue_counter_current = 0
        self.current_subprocesses = 0

        self.configs = configs
        self.settings = settings

        self.global_dnf_repo_cache = {}
        self.data = {}
        self.cache = {}

        self.cache["root_log_deps"] = {}
        self.cache["root_log_deps"]["current"] = {}
        self.cache["root_log_deps"]["next"] = {}

        self.metrics_data = []

        # When analysing buildroot, we don't need metadata about
        # recommends. So this gets flipped and we don't collect them anymore.
        # Saves more than an hour.
        self._global_performance_hack_run_recommends_queries = True

        # Incremental cache: stores workload results keyed by input hash,
        # so unchanged workloads can be skipped on subsequent runs.
        self._repo_fingerprints = {}
        self._cache_hits = 0
        self._cache_misses = 0
        self._incremental_cache_enabled = not settings.get("no_incremental_cache", False)
        self._incremental_cache = {"version": 2, "workloads": {}, "envs": {}}
        if self._incremental_cache_enabled:
            self._load_incremental_cache()
        else:
            log("  Incremental cache disabled (--no-incremental-cache)")

        try:
            self.cache["root_log_deps"]["current"] = load_data(self.settings["root_log_deps_cache_path"])
        except FileNotFoundError:
            pass


    def _record_metric(self, name):
        this_record = {
            "name": name,
            "timestamp": datetime.datetime.now(),
        }
        self.metrics_data.append(this_record)


    def print_metrics(self):
        log("Additional metrics:")

        counter = 0

        for this_record in self.metrics_data:

            if counter == 0:
                prev_timestamp = this_record["timestamp"]
            else:
                self.metrics_data[counter-1]["timestamp"]

            time_diff = this_record["timestamp"] - prev_timestamp

            print("  {} (+{} mins): {}".format(
                this_record["timestamp"].strftime("%H:%M:%S"),
                str(int(time_diff.seconds/60)).zfill(3),
                this_record["name"]
            ))

            counter += 1

    
    def _load_repo_cached(self, base, repo, arch):
        repo_id = repo["id"]

        exists = True
        
        if repo_id not in self.global_dnf_repo_cache:
            exists = False
            self.global_dnf_repo_cache[repo_id] = {}

        elif arch not in self.global_dnf_repo_cache[repo_id]:
            exists = False
        
        if exists:
            #log("  Loading repos from cache...")

            for repo in self.global_dnf_repo_cache[repo_id][arch]:
                base.repos.add(repo)

        else:
            #log("  Loading repos using DNF...")

            for repo_name, repo_data in repo["source"]["repos"].items():
                if repo_data["limit_arches"]:
                    if arch not in repo_data["limit_arches"]:
                        #log("  Skipping {} on {}".format(repo_name, arch))
                        continue
                #log("  Including {}".format(repo_name))

                additional_repo = dnf.repo.Repo(
                    name=repo_name,
                    parent_conf=base.conf
                )
                additional_repo.baseurl = repo_data["baseurl"]
                additional_repo.priority = repo_data["priority"]
                additional_repo.exclude = repo_data["exclude"]
                base.repos.add(additional_repo)

            # Additional repository (if configured)
            #if repo["source"]["additional_repository"]:
            #    additional_repo = dnf.repo.Repo(name="additional-repository",parent_conf=base.conf)
            #    additional_repo.baseurl = [repo["source"]["additional_repository"]]
            #    additional_repo.priority = 1
            #    base.repos.add(additional_repo)

            # All other system repos
            #base.read_all_repos()

            self.global_dnf_repo_cache[repo_id][arch] = []
            for repo in base.repos.iter_enabled():
                self.global_dnf_repo_cache[repo_id][arch].append(repo)


    ###########################################################################
    ### Incremental cache #####################################################
    ###########################################################################

    _INCREMENTAL_CACHE_VERSION = 2
    _INCREMENTAL_CACHE_PATH = "cache_incremental.json.gz"

    def _load_incremental_cache(self):
        try:
            with gzip.open(self._INCREMENTAL_CACHE_PATH, 'rt', encoding='utf-8') as f:
                cache = json.load(f)
            if cache.get("version") == self._INCREMENTAL_CACHE_VERSION:
                self._incremental_cache = cache
                wl_count = len(cache.get("workloads", {}))
                env_count = len(cache.get("envs", {}))
                log(f"  Loaded incremental cache: {wl_count} workloads, {env_count} envs")
            else:
                log("  Incremental cache version mismatch — starting fresh")
        except FileNotFoundError:
            log("  No incremental cache found — will build one after this run")
        except (json.JSONDecodeError, OSError) as e:
            log(f"  Could not load incremental cache ({e}) — starting fresh")

    def _save_incremental_cache(self):
        if not self._incremental_cache_enabled:
            return
        self._incremental_cache["version"] = self._INCREMENTAL_CACHE_VERSION
        self._incremental_cache["repo_fingerprints"] = self._repo_fingerprints
        try:
            with gzip.open(self._INCREMENTAL_CACHE_PATH, 'wt', encoding='utf-8') as f:
                json.dump(self._incremental_cache, f, check_circular=False)
            wl_count = len(self._incremental_cache.get("workloads", {}))
            log(f"  Saved incremental cache ({wl_count} workloads)")
        except (PermissionError, OSError) as e:
            log(f"  Warning: Could not save incremental cache: {e}")

    def _compute_repo_fingerprints(self):
        self._repo_fingerprints = {}
        for repo_id in self.data["pkgs"]:
            for arch in self.data["pkgs"][repo_id]:
                pkg_ids = sorted(self.data["pkgs"][repo_id][arch].keys())
                fp = hashlib.sha256("\n".join(pkg_ids).encode()).hexdigest()[:16]
                self._repo_fingerprints[f"{repo_id}:{arch}"] = fp
        log(f"  Computed repo fingerprints for {len(self._repo_fingerprints)} repo+arch combos")

        old_fps = self._incremental_cache.get("repo_fingerprints", {})
        changed = [k for k in self._repo_fingerprints if self._repo_fingerprints[k] != old_fps.get(k)]
        if changed:
            log(f"  Repo changes detected in: {', '.join(changed)} — affected workloads will be re-analyzed")
        else:
            log("  No repo changes detected — cached workloads are valid")

    @staticmethod
    def _make_hashable(obj):
        if isinstance(obj, set):
            return sorted(str(x) for x in obj)
        if isinstance(obj, dict):
            return {k: Analyzer._make_hashable(v) for k, v in sorted(obj.items())}
        if isinstance(obj, (list, tuple)):
            return [Analyzer._make_hashable(x) for x in obj]
        return obj

    def _workload_cache_key(self, workload_conf, env_conf, repo_id, arch):
        key_data = json.dumps({
            "wc": self._make_hashable(workload_conf),
            "ec": self._make_hashable(env_conf),
            "rf": self._repo_fingerprints.get(f"{repo_id}:{arch}", ""),
            "a": arch,
        }, sort_keys=True, default=str)
        return hashlib.sha256(key_data.encode()).hexdigest()

    def _env_cache_key(self, env_conf, repo_id, arch):
        key_data = json.dumps({
            "ec": self._make_hashable(env_conf),
            "rf": self._repo_fingerprints.get(f"{repo_id}:{arch}", ""),
            "a": arch,
        }, sort_keys=True, default=str)
        return hashlib.sha256(key_data.encode()).hexdigest()

    def _get_cached_workload(self, workload_id, cache_key):
        if not self._incremental_cache_enabled:
            return None
        cached = self._incremental_cache.get("workloads", {}).get(workload_id)
        if cached and cached.get("key") == cache_key:
            self._cache_hits += 1
            return cached["result"]
        self._cache_misses += 1
        return None

    def _put_cached_workload(self, workload_id, cache_key, result):
        if not self._incremental_cache_enabled:
            return
        self._incremental_cache.setdefault("workloads", {})[workload_id] = {
            "key": cache_key,
            "result": result,
        }

    def _get_cached_env(self, env_id, cache_key):
        if not self._incremental_cache_enabled:
            return None
        cached = self._incremental_cache.get("envs", {}).get(env_id)
        if cached and cached.get("key") == cache_key:
            return cached["result"]
        return None

    def _put_cached_env(self, env_id, cache_key, result):
        if not self._incremental_cache_enabled:
            return
        self._incremental_cache.setdefault("envs", {})[env_id] = {
            "key": cache_key,
            "result": result,
        }

    def _pre_check_workload_cache(self):
        """Determine which envs have all their workloads cached.
        
        Returns a dict mapping env_id -> bool (True if all workloads cached).
        This allows skipping env analysis for envs where no fresh workload
        analysis is needed.
        """
        if not self._incremental_cache_enabled:
            log("  Incremental cache disabled — all envs will be analyzed")
            return {}

        workload_env_map = {}
        for workload_conf_id, workload_conf in self.configs["workloads"].items():
            workload_env_map[workload_conf_id] = set()
            for label in workload_conf["labels"]:
                for env_conf_id, env_conf in self.configs["envs"].items():
                    if label in env_conf["labels"]:
                        workload_env_map[workload_conf_id].add(env_conf_id)

        env_all_cached = {}
        for workload_conf_id, workload_conf in self.configs["workloads"].items():
            for env_conf_id in workload_env_map[workload_conf_id]:
                env_conf = self.configs["envs"][env_conf_id]
                for repo_id in env_conf["repositories"]:
                    repo = self.configs["repos"][repo_id]
                    for arch in repo["source"]["architectures"]:
                        env_id = f"{env_conf_id}:{repo_id}:{arch}"
                        workload_id = f"{workload_conf_id}:{env_conf_id}:{repo_id}:{arch}"
                        cache_key = self._workload_cache_key(workload_conf, env_conf, repo_id, arch)
                        is_cached = self._get_cached_workload(workload_id, cache_key) is not None
                        if env_id not in env_all_cached:
                            env_all_cached[env_id] = True
                        if not is_cached:
                            env_all_cached[env_id] = False

        # Reset counters since _get_cached_workload modified them during the pre-check
        self._cache_hits = 0
        self._cache_misses = 0

        cached_count = sum(1 for v in env_all_cached.values() if v)
        total_count = len(env_all_cached)
        log(f"  Pre-check: {cached_count}/{total_count} envs have all workloads cached")
        return env_all_cached


    def _analyze_pkgs(self, repo, arch):
        log("Analyzing pkgs for {repo_name} ({repo_id}) {arch}".format(
                repo_name=repo["name"],
                repo_id=repo["id"],
                arch=arch
            ))
        
        with dnf.Base() as base:

            base.conf.debuglevel = 0
            base.conf.errorlevel = 0
            base.conf.logfilelevel = 0
            base.conf.metadata_expire = -1

            # Local DNF cache
            cachedir_name = "dnf_cachedir-{repo}-{arch}".format(
                repo=repo["id"],
                arch=arch
            )
            base.conf.cachedir = os.path.join(self.tmp_dnf_cachedir, cachedir_name)

            # Generic installroot
            root_name = "dnf_generic_installroot-{repo}-{arch}".format(
                repo=repo["id"],
                arch=arch
            )
            base.conf.installroot = os.path.join(self.tmp_installroots, root_name)

            # Architecture
            base.conf.arch = arch
            base.conf.ignorearch = True

            # Releasever
            base.conf.substitutions['releasever'] = repo["source"]["releasever"]

            for repo_name, repo_data in repo["source"]["repos"].items():
                if repo_data["limit_arches"]:
                    if arch not in repo_data["limit_arches"]:
                        log("  Skipping {} on {}".format(repo_name, arch))
                        continue
                log("  Including {}".format(repo_name))

                additional_repo = dnf.repo.Repo(
                    name=repo_name,
                    parent_conf=base.conf
                )
                additional_repo.baseurl = repo_data["baseurl"]
                additional_repo.priority = repo_data["priority"]
                base.repos.add(additional_repo)

            # Additional repository (if configured)
            #if repo["source"]["additional_repository"]:
            #    additional_repo = dnf.repo.Repo(name="additional-repository",parent_conf=base.conf)
            #    additional_repo.baseurl = [repo["source"]["additional_repository"]]
            #    additional_repo.priority = 1
            #    base.repos.add(additional_repo)

            # Load repos
            log("  Loading repos...")
            #base.read_all_repos()


            # At this stage, I need to get all packages from the repo listed.
            # That also includes modular packages. Modular packages in non-enabled
            # streams would be normally hidden. So I mark all the available repos as
            # hotfix repos to make all packages visible, including non-enabled streams.
            for dnf_repo in base.repos.all():
                dnf_repo.module_hotfixes = True

            # This sometimes fails, so let's try at least N times
            # before totally giving up!
            MAX_TRIES = 10
            attempts = 0
            success = False
            while attempts < MAX_TRIES:
                try:
                    base.fill_sack(load_system_repo=False)
                    success = True
                    break
                except dnf.exceptions.RepoError as err:
                    attempts +=1
                    log("  Failed to download repodata. Trying again!")
            if not success:
                err = "Failed to download repodata while analyzing repo '{repo_name} ({repo_id}) {arch}".format(
                repo_name=repo["name"],
                repo_id=repo["id"],
                arch=arch
                )
                err_log(err)
                raise RepoDownloadError(err)

            # DNF query
            query = base.sack.query

            repo_priorities = {}
            for repo_name, repo_data in repo["source"]["repos"].items():
                repo_priorities[repo_name] = repo_data["priority"]

            all_pkgs_set = set(query())
            pkgs = {}
            for pkg_object in all_pkgs_set:
                pkg_nevra = f"{pkg_object.name}-{pkg_object.evr}.{pkg_object.arch}"
                reponame = pkg_object.reponame

                if pkg_nevra not in pkgs:
                    pkgs[pkg_nevra] = {
                        "id": pkg_nevra,
                        "name": pkg_object.name,
                        "evr": pkg_object.evr,
                        "nevr": f"{pkg_object.name}-{pkg_object.evr}",
                        "arch": pkg_object.arch,
                        "installsize": pkg_object.installsize,
                        "description": pkg_object.description,
                        "summary": pkg_object.summary,
                        "source_name": pkg_object.source_name,
                        "sourcerpm": pkg_object.sourcerpm,
                        "reponame": reponame,
                        "all_reponames": set(),
                    }
                pkgs[pkg_nevra]["all_reponames"].add(reponame)

            for pkg_nevra, pkg in pkgs.items():
                all_reponames = pkg["all_reponames"]
                best_priority = min(repo_priorities[rn] for rn in all_reponames)
                pkg["highest_priority_reponames"] = {rn for rn in all_reponames if repo_priorities[rn] == best_priority}

            log("  Done!  ({pkg_count} packages in total)".format(
                pkg_count=len(pkgs)
            ))
            log("")

        return pkgs
    
    def _analyze_repos(self):
        self.data["repos"] = {}
        for _,repo in self.configs["repos"].items():
            repo_id = repo["id"]
            self.data["pkgs"][repo_id] = {}
            self.data["repos"][repo_id] = {}
            for arch in repo["source"]["architectures"]:
                self.data["pkgs"][repo_id][arch] = self._analyze_pkgs(repo, arch)
            
            # Reading the optional composeinfo
            self.data["repos"][repo_id]["compose_date"] = None
            self.data["repos"][repo_id]["compose_days_ago"] = 0
            if repo["source"]["composeinfo"]:
                # At this point, this is all I can do. Hate me or not, it gets us
                # what we need and won't brake anything in case things go badly. 
                request = urllib.request.Request(repo["source"]["composeinfo"])
                request.add_header("Accept", "application/json")
                request.add_header("User-Agent", "ContentResolver/1.0")
                try:
                    with urllib.request.urlopen(request) as response:
                        composeinfo_raw_response = response.read()

                    composeinfo_data = json.loads(composeinfo_raw_response)
                    self.data["repos"][repo_id]["composeinfo"] = composeinfo_data

                    compose_date = datetime.datetime.strptime(composeinfo_data["payload"]["compose"]["date"], "%Y%m%d").date()
                    self.data["repos"][repo_id]["compose_date"] = compose_date.strftime("%Y-%m-%d")

                    date_now = datetime.datetime.now().date()
                    self.data["repos"][repo_id]["compose_days_ago"] = (date_now - compose_date).days

                except:
                    pass

    def _analyze_package_relations(self, dnf_query, package_placeholders = None):
        relations = {}
        run_recommends = self._global_performance_hack_run_recommends_queries

        pkg_objects = {}
        for pkg in dnf_query:
            pkg_id = f"{pkg.name}-{pkg.evr}.{pkg.arch}"
            pkg_objects[pkg_id] = pkg
            relations[pkg_id] = {
                "required_by": set(),
                "recommended_by": set(),
                "supplements": set(),
                "suggested_by": [],
                "source_name": pkg.source_name,
                "reponame": pkg.reponame,
            }

        # Cache filter(provides=[reldep]) results so each unique reldep
        # is resolved only once. Many packages share the same requires
        # (glibc, python3-libs, etc.), so this avoids thousands of
        # redundant libsolv lookups.
        provides_cache = {}

        def _get_provider_ids(reldep):
            key = str(reldep)
            if key not in provides_cache:
                result = set()
                for prov_pkg in dnf_query.filter(provides=[reldep]):
                    result.add(f"{prov_pkg.name}-{prov_pkg.evr}.{prov_pkg.arch}")
                provides_cache[key] = result
            return provides_cache[key]

        for pkg_id, pkg in pkg_objects.items():
            for req in pkg.requires:
                for provider_id in _get_provider_ids(req):
                    if provider_id in relations and provider_id != pkg_id:
                        relations[provider_id]["required_by"].add(pkg_id)

        if run_recommends:
            for pkg_id, pkg in pkg_objects.items():
                for rec in pkg.recommends:
                    for provider_id in _get_provider_ids(rec):
                        if provider_id in relations and provider_id != pkg_id:
                            relations[provider_id]["recommended_by"].add(pkg_id)

        for pkg_id, pkg in pkg_objects.items():
            for supplement_reldep in pkg.supplements:
                for provider_id in _get_provider_ids(supplement_reldep):
                    if provider_id in relations:
                        relations[pkg_id]["supplements"].add(provider_id)

        for pkg_id, rel in relations.items():
            rel["required_by"] = sorted(rel["required_by"])
            rel["recommended_by"] = sorted(rel["recommended_by"])
            rel["supplements"] = sorted(rel["supplements"])

        if package_placeholders:
            name_to_ids = {}
            for pkg_id in relations:
                pname = pkg_id.rsplit("-", 2)[0]
                name_to_ids.setdefault(pname, []).append(pkg_id)

            for placeholder_name, placeholder_data in package_placeholders.items():
                placeholder_id = pkg_placeholder_name_to_id(placeholder_name)
                relations[placeholder_id] = {
                    "required_by": [], "recommended_by": [],
                    "suggested_by": [], "supplements": [], "reponame": None,
                }

            for placeholder_name, placeholder_data in package_placeholders.items():
                placeholder_id = pkg_placeholder_name_to_id(placeholder_name)
                for dep_name in placeholder_data["requires"]:
                    for pkg_id in name_to_ids.get(dep_name, []):
                        relations[pkg_id]["required_by"].append(placeholder_id)

        return relations


    def _analyze_env_without_leaking(self, env_conf, repo, arch):

        # DNF leaks memory and file descriptors :/
        # 
        # So, this workaround runs it in a subprocess that should have its resources
        # freed when done!

        queue_result = multiprocessing.Queue()
        process = multiprocessing.Process(target=self._analyze_env_process, args=(queue_result, env_conf, repo, arch))
        process.start()
        process.join()

        # This basically means there was an exception in the processing and the process crashed
        if queue_result.empty():
            raise AnalysisError
        
        env = queue_result.get()

        return env


    def _analyze_env_process(self, queue_result, env_conf, repo, arch):

        env = self._analyze_env(env_conf, repo, arch)
        queue_result.put(env)


    def _analyze_env(self, env_conf, repo, arch):
        env = {}
        
        env["env_conf_id"] = env_conf["id"]
        env["pkg_ids"] = []
        env["repo_id"] = repo["id"]
        env["arch"] = arch

        env["pkg_relations"] = []

        env["errors"] = {}
        env["errors"]["non_existing_pkgs"] = []

        env["succeeded"] = True

        with dnf.Base() as base:

            base.conf.debuglevel = 0
            base.conf.errorlevel = 0
            base.conf.logfilelevel = 0
            base.conf.metadata_expire = -1

            # Local DNF cache
            cachedir_name = "dnf_cachedir-{repo}-{arch}".format(
                repo=repo["id"],
                arch=arch
            )
            base.conf.cachedir = os.path.join(self.tmp_dnf_cachedir, cachedir_name)

            # Environment installroot
            root_name = "dnf_env_installroot-{env_conf}-{repo}-{arch}".format(
                env_conf=env_conf["id"],
                repo=repo["id"],
                arch=arch
            )
            base.conf.installroot = os.path.join(self.tmp_installroots, root_name)

            # Architecture
            base.conf.arch = arch
            base.conf.ignorearch = True

            # Releasever
            base.conf.substitutions['releasever'] = repo["source"]["releasever"]

            # Additional DNF Settings
            base.conf.tsflags.append('justdb')
            base.conf.tsflags.append('noscripts')

            # Environment config
            if "include-weak-deps" not in env_conf["options"]:
                base.conf.install_weak_deps = False
            if "include-docs" not in env_conf["options"]:
                base.conf.tsflags.append('nodocs')

            # Load repos
            #log("  Loading repos...")
            #base.read_all_repos()
            self._load_repo_cached(base, repo, arch)

            # This sometimes fails, so let's try at least N times
            # before totally giving up!
            MAX_TRIES = 10
            attempts = 0
            success = False
            while attempts < MAX_TRIES:
                try:
                    base.fill_sack(load_system_repo=False)
                    success = True
                    break
                except dnf.exceptions.RepoError as err:
                    attempts +=1
                    log("  Failed to download repodata. Trying again!")
            if not success:
                err = "Failed to download repodata while analyzing environment '{env_conf}' from '{repo}' {arch}:".format(
                    env_conf=env_conf["id"],
                    repo=repo["id"],
                    arch=arch
                )
                err_log(err)
                raise RepoDownloadError(err)


            # Packages
            log("  Adding packages...")
            for pkg in env_conf["packages"]:
                try:
                    base.install(pkg)
                except dnf.exceptions.MarkingError:
                    env["errors"]["non_existing_pkgs"].append(pkg)
                    continue
            
            # Groups
            log("  Adding groups...")
            if env_conf["groups"]:
                base.read_comps(arch_filter=True)
            for grp_spec in env_conf["groups"]:
                group = base.comps.group_by_pattern(grp_spec)
                if not group:
                    env["errors"]["non_existing_pkgs"].append(grp_spec)
                    continue
                base.group_install(group.id, ['mandatory', 'default'])

            # Architecture-specific packages
            for pkg in env_conf["arch_packages"][arch]:
                try:
                    base.install(pkg)
                except dnf.exceptions.MarkingError:
                    env["errors"]["non_existing_pkgs"].append(pkg)
                    continue
            
            # Resolve dependencies
            log("  Resolving dependencies...")
            try:
                base.resolve()
            except dnf.exceptions.DepsolveError as err:
                err_log("Failed to analyze environment '{env_conf}' from '{repo}' {arch}:".format(
                        env_conf=env_conf["id"],
                        repo=repo["id"],
                        arch=arch
                    ))
                err_log("  - {err}".format(err=err))
                env["succeeded"] = False
                env["errors"]["message"] = str(err)
                return env

            # Write the result into RPMDB.
            # The transaction needs us to download all the packages. :(
            # So let's do that to make it happy.
            log("  Downloading packages...")
            try:
                base.download_packages(base.transaction.install_set)
            except dnf.exceptions.DownloadError as err:
                err_log("Failed to analyze environment '{env_conf}' from '{repo}' {arch}:".format(
                        env_conf=env_conf["id"],
                        repo=repo["id"],
                        arch=arch
                    ))
                err_log("  - {err}".format(err=err))
                env["succeeded"] = False
                env["errors"]["message"] = str(err)
                return env

            log("  Running DNF transaction, writing RPMDB...")
            try:
                base.do_transaction()
            except (dnf.exceptions.TransactionCheckError, dnf.exceptions.Error) as err:
                err_log("Failed to analyze environment '{env_conf}' from '{repo}' {arch}:".format(
                        env_conf=env_conf["id"],
                        repo=repo["id"],
                        arch=arch
                    ))
                err_log("  - {err}".format(err=err))
                env["succeeded"] = False
                env["errors"]["message"] = str(err)
                return env

            # DNF Query
            log("  Creating a DNF Query object...")
            query = base.sack.query().filterm(pkg=base.transaction.install_set)

            for pkg in query:
                pkg_id = "{name}-{evr}.{arch}".format(
                    name=pkg.name,
                    evr=pkg.evr,
                    arch=pkg.arch
                )
                env["pkg_ids"].append(pkg_id)
            
            env["pkg_relations"] = self._analyze_package_relations(query)

            log("  Done!  ({pkg_count} packages in total)".format(
                pkg_count=len(env["pkg_ids"])
            ))
            log("")
        
        return env


    def _analyze_envs(self, env_all_cached=None):
        envs = {}

        env_tasks = []
        for env_conf_id, env_conf in self.configs["envs"].items():
            for repo_id in env_conf["repositories"]:
                repo = self.configs["repos"][repo_id]
                for arch in repo["source"]["architectures"]:
                    env_id = f"{env_conf_id}:{repo_id}:{arch}"
                    env_tasks.append((env_id, env_conf, repo, arch))

        cached_count = 0
        for env_id, env_conf, repo, arch in env_tasks:
            cache_key = self._env_cache_key(env_conf, repo["id"], arch)
            all_wl_cached = env_all_cached.get(env_id, False) if env_all_cached else False

            if all_wl_cached:
                cached_env = self._get_cached_env(env_id, cache_key)
                if cached_env is not None:
                    envs[env_id] = cached_env
                    cached_count += 1
                    continue

            log(f"Analyzing {env_conf['name']} ({env_conf['id']}) from {repo['name']} ({repo['id']}) {arch}...")
            envs[env_id] = self._analyze_env(env_conf, repo, arch)
            self._put_cached_env(env_id, cache_key, envs[env_id])

        if cached_count:
            log(f"  Skipped {cached_count} envs (all their workloads are cached)")

        self.data["envs"] = envs


    def _return_failed_workload_env_err(self, workload_conf, env_conf, repo, arch):
        workload = {}

        workload["workload_conf_id"] = workload_conf["id"]
        workload["env_conf_id"] = env_conf["id"]
        workload["repo_id"] = repo["id"]
        workload["arch"] = arch
        workload["labels"] = list(set(workload_conf["labels"]) & set(env_conf["labels"]))

        workload["pkg_env_ids"] = []
        workload["pkg_added_ids"] = []
        workload["pkg_placeholder_ids"] = []
        workload["srpm_placeholder_names"] = []

        workload["pkg_relations"] = []

        workload["errors"] = {}
        workload["errors"]["non_existing_pkgs"] = []
        workload["succeeded"] = False
        workload["env_succeeded"] = False

        workload["warnings"] = {}
        workload["warnings"]["non_existing_pkgs"] = []
        workload["warnings"]["non_existing_placeholder_deps"] = []
        workload["warnings"]["message"] = None

        workload["errors"]["message"] = """
        Failed to analyze this workload because of an error while analyzing the environment.

        Please see the associated environment results for a detailed error message.
        """

        return workload


    def _analyze_workload(self, workload_conf, env_conf, repo, arch):

        workload = {}

        workload["workload_conf_id"] = workload_conf["id"]
        workload["env_conf_id"] = env_conf["id"]
        workload["repo_id"] = repo["id"]
        workload["arch"] = arch

        workload["pkg_env_ids"] = []
        workload["pkg_added_ids"] = []
        workload["pkg_placeholder_ids"] = []
        workload["srpm_placeholder_names"] = []

        workload["pkg_relations"] = []

        workload["errors"] = {}
        workload["errors"]["non_existing_pkgs"] = []
        workload["errors"]["non_existing_placeholder_deps"] = []

        workload["warnings"] = {}
        workload["warnings"]["non_existing_pkgs"] = []
        workload["warnings"]["non_existing_placeholder_deps"] = []
        workload["warnings"]["message"] = None

        workload["succeeded"] = True
        workload["env_succeeded"] = True


        # Figure out the workload labels
        # It can only have labels that are in both the workload_conf and the env_conf
        workload["labels"] = list(set(workload_conf["labels"]) & set(env_conf["labels"]))

        with dnf.Base() as base:

            base.conf.debuglevel = 0
            base.conf.errorlevel = 0
            base.conf.logfilelevel = 0
            base.conf.metadata_expire = -1

            # Local DNF cache
            cachedir_name = "dnf_cachedir-{repo}-{arch}".format(
                repo=repo["id"],
                arch=arch
            )
            base.conf.cachedir = os.path.join(self.tmp_dnf_cachedir, cachedir_name)

            # Environment installroot
            # Since we're not writing anything into the installroot,
            # let's just use the base image's installroot!
            root_name = "dnf_env_installroot-{env_conf}-{repo}-{arch}".format(
                env_conf=env_conf["id"],
                repo=repo["id"],
                arch=arch
            )
            base.conf.installroot = os.path.join(self.tmp_installroots, root_name)

            # Architecture
            base.conf.arch = arch
            base.conf.ignorearch = True

            # Releasever
            base.conf.substitutions['releasever'] = repo["source"]["releasever"]

            # Environment config
            if "include-weak-deps" not in workload_conf["options"]:
                base.conf.install_weak_deps = False
            if "include-docs" not in workload_conf["options"]:
                base.conf.tsflags.append('nodocs')

            # Load repos
            #log("  Loading repos...")
            #base.read_all_repos()
            self._load_repo_cached(base, repo, arch)

            # 0 % 

            # Now I need to load the local RPMDB.
            # However, if the environment is empty, it wasn't created, so I need to treat
            # it differently. So let's check!
            if len(env_conf["packages"]) or len(env_conf["arch_packages"][arch]) or len(env_conf["groups"]):
                # It's not empty! Load local data.
                base.fill_sack(load_system_repo=True)
            else:
                # It's empty. Treat it like we're using an empty installroot.
                # This sometimes fails, so let's try at least N times
                # before totally giving up!
                MAX_TRIES = 10
                attempts = 0
                success = False
                while attempts < MAX_TRIES:
                    try:
                        base.fill_sack(load_system_repo=False)
                        success = True
                        break
                    except dnf.exceptions.RepoError as err:
                        attempts +=1
                        #log("  Failed to download repodata. Trying again!")
                if not success:
                    err = "Failed to download repodata while analyzing workload '{workload_id} on '{env_id}' from '{repo}' {arch}...".format(
                            workload_id=workload_conf_id,
                            env_id=env_conf_id,
                            repo_name=repo["name"],
                            repo=repo_id,
                            arch=arch)
                    err_log(err)
                    raise RepoDownloadError(err)
            
            # 37 %

            # Packages
            #log("  Adding packages...")
            for pkg in workload_conf["packages"]:
                try:
                    base.install(pkg)
                except dnf.exceptions.MarkingError:
                    if pkg in self.settings["weird_packages_that_can_not_be_installed"]:
                        continue
                    else:
                        if "strict" in workload_conf["options"]:
                            workload["errors"]["non_existing_pkgs"].append(pkg)
                        else:
                            workload["warnings"]["non_existing_pkgs"].append(pkg)
                        continue
            
            # Groups
            #log("  Adding groups...")
            if workload_conf["groups"]:
                base.read_comps(arch_filter=True)
            for grp_spec in workload_conf["groups"]:
                group = base.comps.group_by_pattern(grp_spec)
                if not group:
                    workload["errors"]["non_existing_pkgs"].append(grp_spec)
                    continue
                base.group_install(group.id, ['mandatory', 'default'])
            
            
                # TODO: Mark group packages as required... the following code doesn't work
                #for pkg in group.packages_iter():
                #    print(pkg.name)
                #    workload_conf["packages"].append(pkg.name)
                
                    
            
            # Filter out the relevant package placeholders for this arch
            package_placeholders = {}
            for placeholder_name, placeholder_data in workload_conf["package_placeholders"]["pkgs"].items():
                # If this placeholder is not limited to just a usbset of arches, add it
                if not placeholder_data["limit_arches"]:
                    package_placeholders[placeholder_name] = placeholder_data
                # otherwise it is limited. In that case, only add it if the current arch is on its list
                elif arch in placeholder_data["limit_arches"]:
                    package_placeholders[placeholder_name] = placeholder_data
            
            # Same for SRPM placeholders
            srpm_placeholders = {}
            for placeholder_name, placeholder_data in workload_conf["package_placeholders"]["srpms"].items():
                # If this placeholder is not limited to just a usbset of arches, add it
                if not placeholder_data["limit_arches"]:
                    srpm_placeholders[placeholder_name] = placeholder_data
                # otherwise it is limited. In that case, only add it if the current arch is on its list
                elif arch in placeholder_data["limit_arches"]:
                    srpm_placeholders[placeholder_name] = placeholder_data

            # Dependencies of package placeholders
            #log("  Adding package placeholder dependencies...")
            for placeholder_name, placeholder_data in package_placeholders.items():
                for pkg in placeholder_data["requires"]:
                    try:
                        base.install(pkg)
                    except dnf.exceptions.MarkingError:
                        if "strict" in workload_conf["options"]:
                            workload["errors"]["non_existing_placeholder_deps"].append(pkg)
                        else:
                            workload["warnings"]["non_existing_placeholder_deps"].append(pkg)
                        continue

            # Architecture-specific packages
            for pkg in workload_conf["arch_packages"][arch]:
                try:
                    base.install(pkg)
                except dnf.exceptions.MarkingError:
                    if "strict" in workload_conf["options"]:
                        workload["errors"]["non_existing_pkgs"].append(pkg)
                    else:
                        workload["warnings"]["non_existing_pkgs"].append(pkg)
                    continue

            if workload["errors"]["non_existing_pkgs"] or workload["errors"]["non_existing_placeholder_deps"]:
                error_message_list = []
                if workload["errors"]["non_existing_pkgs"]:
                    error_message_list.append("The following required packages are not available:")
                    for pkg_name in workload["errors"]["non_existing_pkgs"]:
                        pkg_string = "  - {pkg_name}".format(
                            pkg_name=pkg_name
                        )
                        error_message_list.append(pkg_string)
                if workload["errors"]["non_existing_placeholder_deps"]:
                    error_message_list.append("The following dependencies of package placeholders are not available:")
                    for pkg_name in workload["errors"]["non_existing_placeholder_deps"]:
                        pkg_string = "  - {pkg_name}".format(
                            pkg_name=pkg_name
                        )
                        error_message_list.append(pkg_string)
                error_message = "\n".join(error_message_list)
                workload["succeeded"] = False
                workload["errors"]["message"] = str(error_message)
                #log("  Failed!  (Error message will be on the workload results page.")
                #log("")
                return workload
            
            if workload["warnings"]["non_existing_pkgs"] or workload["warnings"]["non_existing_placeholder_deps"]:
                error_message_list = []
                if workload["warnings"]["non_existing_pkgs"]:
                    error_message_list.append("The following required packages are not available (and were skipped):")
                    for pkg_name in workload["warnings"]["non_existing_pkgs"]:
                        pkg_string = "  - {pkg_name}".format(
                            pkg_name=pkg_name
                        )
                        error_message_list.append(pkg_string)
                if workload["warnings"]["non_existing_placeholder_deps"]:
                    error_message_list.append("The following dependencies of package placeholders are not available (and were skipped):")
                    for pkg_name in workload["warnings"]["non_existing_placeholder_deps"]:
                        pkg_string = "  - {pkg_name}".format(
                            pkg_name=pkg_name
                        )
                        error_message_list.append(pkg_string)
                error_message = "\n".join(error_message_list)
                workload["warnings"]["message"] = str(error_message)

            # 37 %

            # Resolve dependencies
            #log("  Resolving dependencies...")
            try:
                base.resolve()
            except dnf.exceptions.DepsolveError as err:
                workload["succeeded"] = False
                workload["errors"]["message"] = str(err)
                #log("  Failed!  (Error message will be on the workload results page.")
                #log("")
                return workload

            # 43 %

            # DNF Query
            #log("  Creating a DNF Query object...")
            query_env = base.sack.query()
            pkgs_env = set(query_env.installed())
            pkgs_added = set(base.transaction.install_set)
            pkgs_all = set.union(pkgs_env, pkgs_added)
            query_all = base.sack.query().filterm(pkg=pkgs_all)
            
            # OK all good so save stuff now
            for pkg in pkgs_env:
                pkg_id = "{name}-{evr}.{arch}".format(
                    name=pkg.name,
                    evr=pkg.evr,
                    arch=pkg.arch
                )
                workload["pkg_env_ids"].append(pkg_id)
            
            for pkg in pkgs_added:
                pkg_id = "{name}-{evr}.{arch}".format(
                    name=pkg.name,
                    evr=pkg.evr,
                    arch=pkg.arch
                )
                workload["pkg_added_ids"].append(pkg_id)

            # No errors so far? That means the analysis has succeeded,
            # so placeholders can be added to the list as well.
            # (Failed workloads need to have empty results, that's why)
            for placeholder_name in package_placeholders:
                workload["pkg_placeholder_ids"].append(pkg_placeholder_name_to_id(placeholder_name))
            
            for srpm_placeholder_name in srpm_placeholders:
                workload["srpm_placeholder_names"].append(srpm_placeholder_name)

            # 43 %

            workload["pkg_relations"] = self._analyze_package_relations(query_all, package_placeholders)

            # 100 %
            
            pkg_env_count = len(workload["pkg_env_ids"])
            pkg_added_count = len(workload["pkg_added_ids"])
            #log("  Done!  ({pkg_count} packages in total. That's {pkg_env_count} in the environment, and {pkg_added_count} added.)".format(
            #    pkg_count=str(pkg_env_count + pkg_added_count),
            #    pkg_env_count=pkg_env_count,
            #    pkg_added_count=pkg_added_count
            #))
            #log("")

        # How long do various parts take:
        # 37 % - populatind DNF's base.sack
        # 6 %  - resolving deps
        # 57 % - _analyze_package_relations with recommends

        # Removing recommends from _analyze_package_relations 
        # gets the total duration down to
        # 64 %

        return workload

    
    def _analyze_workload_process(self, queue_result, workload_conf, env_conf, repo, arch):

        workload = self._analyze_workload(workload_conf, env_conf, repo, arch)
        queue_result.put(workload)


    async def _analyze_workloads_subset_async(self, task_queue, results):

        for task in task_queue:
            workload_conf = task["workload_conf"]
            env_conf = task["env_conf"]
            repo = task["repo"]
            arch = task["arch"]

            workload_id = "{workload_conf_id}:{env_conf_id}:{repo_id}:{arch}".format(
                workload_conf_id=workload_conf["id"],
                env_conf_id=env_conf["id"],
                repo_id=repo["id"],
                arch=arch
            )

            # Check incremental cache
            cache_key = self._workload_cache_key(workload_conf, env_conf, repo["id"], arch)
            cached_result = self._get_cached_workload(workload_id, cache_key)
            if cached_result is not None:
                self.workload_queue_counter_current += 1
                results[workload_id] = cached_result
                continue

            # Max processes
            while True:
                if self.current_subprocesses < self.settings["max_subprocesses"]:
                    self.current_subprocesses += 1
                    break
                else:
                    await asyncio.sleep(.1)

            # Log progress
            self.workload_queue_counter_current += 1
            log("[{} of {}]".format(self.workload_queue_counter_current, self.workload_queue_counter_total))
            log("Analyzing workload: {}".format(workload_id))
            log("")

            queue_result = multiprocessing.Queue()
            process = multiprocessing.Process(target=self._analyze_workload_process, args=(queue_result, workload_conf, env_conf, repo, arch), daemon=True)
            process.start()

            # 2 seconds
            for _ in range(1, 20):
                if queue_result.empty():
                    await asyncio.sleep(.1)
                else:
                    break
            
            # 20 seconds
            for _ in range(1, 20):
                if queue_result.empty():
                    await asyncio.sleep(1)
                else:
                    break
            
            # 200 seconds
            for _ in range(1, 20):
                if queue_result.empty():
                    await asyncio.sleep(10)
                else:
                    break

            self.current_subprocesses -= 1

            if queue_result.empty():
                log("")
                log("")
                log("--------------------------------------------------------------------------")
                log("")
                log("ERROR: Workload analysis failed")
                log("")
                log("Details:")
                log("  workload_conf: {}".format(workload_conf["id"]))
                log("  env_conf:      {}".format(env_conf["id"]))
                log("  repo:          {}".format(repo["id"]))
                log("  arch:          {}".format(arch))
                log("")
                log("More details somewhere above.")
                log("")
                log("--------------------------------------------------------------------------")
                log("")
                log("")
                sys.exit(1)
        
            workload = queue_result.get()
            results[workload_id] = workload

            # Store in incremental cache
            self._put_cached_workload(workload_id, cache_key, workload)


    async def _analyze_workloads_async(self, results):

        hits_before = self._cache_hits

        tasks = []

        for repo in self.workload_queue:
            for arch in self.workload_queue[repo]:

                task_queue = self.workload_queue[repo][arch]

                tasks.append(asyncio.create_task(self._analyze_workloads_subset_async(task_queue, results)))
        
        for task in tasks:
            await task

        batch_hits = self._cache_hits - hits_before
        if batch_hits:
            log(f"  ({batch_hits} workloads loaded from incremental cache)")

        # Periodically save cache after each batch of workloads so partial runs
        # still build the cache (the final save at end of analyze_things covers
        # the complete state).
        if self._incremental_cache_enabled and self._cache_misses > 0:
            self._save_incremental_cache()

        log("DONE!")

    
    def _queue_workload_processing(self, workload_conf, env_conf, repo, arch):
        
        repo_id = repo["id"]

        if repo_id not in self.workload_queue:
            self.workload_queue[repo_id] = {}
        
        if arch not in self.workload_queue[repo_id]:
            self.workload_queue[repo_id][arch] = []

        workload_task = {
            "workload_conf": workload_conf,
            "env_conf" : env_conf,
            "repo" : repo,
            "arch" : arch
        }

        self.workload_queue[repo_id][arch].append(workload_task)
        self.workload_queue_counter_total += 1


    def _reset_workload_processing_queue(self):
        self.workload_queue = {}
        self.workload_queue_counter_total = 0
        self.workload_queue_counter_current = 0


    def _analyze_workloads(self):

        # Initialise
        self.data["workloads"] = {}
        self._reset_workload_processing_queue()

        # Here, I need to mix and match workloads & envs based on labels
        workload_env_map = {}
        # Look at all workload configs...
        for workload_conf_id, workload_conf in self.configs["workloads"].items():
            workload_env_map[workload_conf_id] = set()
            # ... and all of their labels.
            for label in workload_conf["labels"]:
                # And for each label, find all env configs...
                for env_conf_id, env_conf in self.configs["envs"].items():
                    # ... that also have the label.
                    if label in env_conf["labels"]:
                        # And save those.
                        workload_env_map[workload_conf_id].add(env_conf_id)
        
        # And now, look at all workload configs...
        for workload_conf_id, workload_conf in self.configs["workloads"].items():
            # ... and for each, look at all env configs it should be analyzed in.
            for env_conf_id in workload_env_map[workload_conf_id]:
                # Each of those envs can have multiple repos associated...
                env_conf = self.configs["envs"][env_conf_id]
                for repo_id in env_conf["repositories"]:
                    # ... and each repo probably has multiple architecture.
                    repo = self.configs["repos"][repo_id]
                    for arch in repo["source"]["architectures"]:

                        # And now it has:
                        #   all workload configs *
                        #   all envs that match those *
                        #   all repos of those envs *
                        #   all arches of those repos.
                        # That's a lot of stuff! Let's analyze all of that!

                        # Before even started, look if the env succeeded. If not, there's
                        # no point in doing anything here.
                        env_id = "{env_conf_id}:{repo_id}:{arch}".format(
                            env_conf_id=env_conf["id"],
                            repo_id=repo["id"],
                            arch=arch
                        )
                        env = self.data["envs"][env_id]

                        if env["succeeded"]:
                            self._queue_workload_processing(workload_conf, env_conf, repo, arch)

                        else:
                            workload_id = "{workload_conf_id}:{env_conf_id}:{repo_id}:{arch}".format(
                                workload_conf_id=workload_conf_id,
                                env_conf_id=env_conf_id,
                                repo_id=repo_id,
                                arch=arch
                            )
                            self.data["workloads"][workload_id] = self._return_failed_workload_env_err(workload_conf, env_conf, repo, arch)

        asyncio.run(self._analyze_workloads_async(self.data["workloads"]))


    def _init_view_pkg(self, input_pkg, arch, placeholder=False, level=0):
        if placeholder:
            pkg = {
                "id": pkg_placeholder_name_to_id(input_pkg["name"]),
                "name": input_pkg["name"],
                "evr": "000-placeholder",
                "nevr": pkg_placeholder_name_to_nevr(input_pkg["name"]),
                "arch": "placeholder",
                "installsize": 0,
                "description": input_pkg["description"],
                "summary": input_pkg["description"],
                "source_name": input_pkg["srpm"],
                "sourcerpm": "{}-000-placeholder".format(input_pkg["srpm"]),
                "q_arch": input_pkg,
                "reponame": "n/a",
                "all_reponames": set(),
                "highest_priority_reponames": set()
            }

        else:
            pkg = dict(input_pkg)

        pkg["view_arch"] = arch

        pkg["placeholder"] = placeholder

        pkg["in_workload_ids_all"] = set()
        pkg["in_workload_ids_req"] = set()
        pkg["in_workload_ids_dep"] = set()
        pkg["in_workload_ids_env"] = set()

        pkg["in_buildroot_of_srpm_id_all"] = set()
        pkg["in_buildroot_of_srpm_id_req"] = set()
        pkg["in_buildroot_of_srpm_id_dep"] = set()
        pkg["in_buildroot_of_srpm_id_env"] = set()

        pkg["unwanted_completely_in_list_ids"] = set()
        pkg["unwanted_buildroot_in_list_ids"] = set()

        pkg["level"] = []

        # Level 0 is runtime
        pkg["level"].append({
            "all": pkg["in_workload_ids_all"],
            "req": pkg["in_workload_ids_req"],
            "dep": pkg["in_workload_ids_dep"],
            "env": pkg["in_workload_ids_env"],
        })

        # Level 1 and higher is buildroot
        for _ in range(level):
            pkg["level"].append({
                "all": set(),
                "req": set(),
                "dep": set(),
                "env": set()
            })

        pkg["required_by"] = set()
        pkg["recommended_by"] = set()
        pkg["suggested_by"] = set()
        pkg["supplements"] = set()

        return pkg


    def _init_view_srpm(self, pkg, level=0):

        srpm_id = pkg["sourcerpm"].rsplit(".src.rpm")[0]

        srpm = {}
        srpm["id"] = srpm_id
        srpm["name"] = pkg["source_name"]
        srpm["reponame"] = pkg["reponame"]
        srpm["pkg_ids"] = set()

        srpm["placeholder"] = False
        srpm["placeholder_directly_required_pkg_names"] = []

        srpm["in_workload_ids_all"] = set()
        srpm["in_workload_ids_req"] = set()
        srpm["in_workload_ids_dep"] = set()
        srpm["in_workload_ids_env"] = set()

        srpm["in_buildroot_of_srpm_id_all"] = set()
        srpm["in_buildroot_of_srpm_id_req"] = set()
        srpm["in_buildroot_of_srpm_id_dep"] = set()
        srpm["in_buildroot_of_srpm_id_env"] = set()

        srpm["unwanted_completely_in_list_ids"] = set()
        srpm["unwanted_buildroot_in_list_ids"] = set()

        srpm["level"] = []

        # Level 0 is runtime
        srpm["level"].append({
            "all": srpm["in_workload_ids_all"],
            "req": srpm["in_workload_ids_req"],
            "dep": srpm["in_workload_ids_dep"],
            "env": srpm["in_workload_ids_env"],
        })

        # Level 1 and higher is buildroot
        for _ in range(level):
            srpm["level"].append({
                "all": set(),
                "req": set(),
                "dep": set(),
                "env": set()
            })

        return srpm


    def _analyze_view(self, view_conf, arch, views):
        view_conf_id = view_conf["id"]

        log("Analyzing view: {view_name} ({view_conf_id}) for {arch}".format(
            view_name=view_conf["name"],
            view_conf_id=view_conf_id,
            arch=arch
        ))

        view_id = "{view_conf_id}:{arch}".format(
            view_conf_id=view_conf_id,
            arch=arch
        )

        repo_id = view_conf["repository"]

        # Setting up the data buckets for this view
        view = {}

        view["id"] = view_id
        view["view_conf_id"] = view_conf_id
        view["arch"] = arch

        view["workload_ids"] = []
        view["pkgs"] = {}
        view["source_pkgs"] = {}

        # Workloads
        for workload_id, workload in self.data["workloads"].items():
            if workload["repo_id"] != repo_id:
                continue
            
            if workload["arch"] != arch:
                continue

            if not set(workload["labels"]) & set(view_conf["labels"]):
                continue

            view["workload_ids"].append(workload_id)

        log("  Includes {} workloads.".format(len(view["workload_ids"])))

        # Packages
        for workload_id in view["workload_ids"]:
            workload = self.data["workloads"][workload_id]
            workload_conf_id = workload["workload_conf_id"]
            workload_conf = self.configs["workloads"][workload_conf_id]

            view_pkgs = view["pkgs"]
            repo_pkgs = self.data["pkgs"][repo_id][arch]
            wl_relations = workload["pkg_relations"]
            wl_packages = workload_conf["packages"]
            wl_arch_packages = workload_conf["arch_packages"][arch]

            for pkg_id in workload["pkg_env_ids"]:
                if pkg_id not in view_pkgs:
                    view_pkgs[pkg_id] = self._init_view_pkg(repo_pkgs[pkg_id], arch)

                vp = view_pkgs[pkg_id]
                vp["in_workload_ids_all"].add(workload_id)
                vp["in_workload_ids_env"].add(workload_id)

                if vp["name"] in wl_packages or vp["name"] in wl_arch_packages:
                    vp["in_workload_ids_req"].add(workload_id)

                rel = wl_relations[pkg_id]
                vp["required_by"].update(rel["required_by"])
                vp["recommended_by"].update(rel["recommended_by"])
                vp["suggested_by"].update(rel["suggested_by"])
                vp["supplements"].update(rel["supplements"])

            for pkg_id in workload["pkg_added_ids"]:
                if pkg_id not in view_pkgs:
                    view_pkgs[pkg_id] = self._init_view_pkg(repo_pkgs[pkg_id], arch)

                vp = view_pkgs[pkg_id]
                vp["in_workload_ids_all"].add(workload_id)

                if vp["name"] in wl_packages or vp["name"] in wl_arch_packages:
                    vp["in_workload_ids_req"].add(workload_id)
                else:
                    vp["in_workload_ids_dep"].add(workload_id)

                rel = wl_relations[pkg_id]
                vp["required_by"].update(rel["required_by"])
                vp["recommended_by"].update(rel["recommended_by"])
                vp["suggested_by"].update(rel["suggested_by"])
                vp["supplements"].update(rel["supplements"])

            # And finally the non-existing, imaginary, package placeholders!
            for pkg_id in workload["pkg_placeholder_ids"]:

                # Initialise
                if pkg_id not in view["pkgs"]:
                    placeholder = workload_conf["package_placeholders"]["pkgs"][pkg_id_to_name(pkg_id)]
                    view["pkgs"][pkg_id] = self._init_view_pkg(placeholder, arch, placeholder=True)
                
                # It's in this wokrload
                view["pkgs"][pkg_id]["in_workload_ids_all"].add(workload_id)

                # Placeholders are by definition required
                view["pkgs"][pkg_id]["in_workload_ids_req"].add(workload_id)
            
            # ... including the SRPM placeholders
            for srpm_name in workload["srpm_placeholder_names"]:
                srpm_id = pkg_placeholder_name_to_nevr(srpm_name)

                # Initialise
                if srpm_id not in view["source_pkgs"]:
                    sourcerpm = "{}.src.rpm".format(srpm_id)
                    view["source_pkgs"][srpm_id] = self._init_view_srpm({"sourcerpm": sourcerpm, "source_name": srpm_name, "reponame": None})
                
                # It's a placeholder
                view["source_pkgs"][srpm_id]["placeholder"] = True

                # Build requires
                view["source_pkgs"][srpm_id]["placeholder_directly_required_pkg_names"] = workload_conf["package_placeholders"]["srpms"][srpm_name]["buildrequires"]
        
        # If this is an addon view, remove all packages that are already in the parent view
        if view_conf["type"] == "addon":
            base_view_conf_id = view_conf["base_view_id"]

            base_view_id = "{base_view_conf_id}:{arch}".format(
                base_view_conf_id=base_view_conf_id,
                arch=arch
            )

            for base_view_pkg_id in views[base_view_id]["pkgs"]:
                if base_view_pkg_id in view["pkgs"]:
                    del view["pkgs"][base_view_pkg_id]

        # Done with packages!
        log("  Includes {} packages.".format(len(view["pkgs"])))

        # But not with source packages, that's an entirely different story!
        for pkg_id, pkg in view["pkgs"].items():
            srpm_id = pkg["sourcerpm"].rsplit(".src.rpm")[0]

            if srpm_id not in view["source_pkgs"]:
                view["source_pkgs"][srpm_id] = self._init_view_srpm(pkg)

            # Include some information from the RPM
            view["source_pkgs"][srpm_id]["pkg_ids"].add(pkg_id)

            view["source_pkgs"][srpm_id]["in_workload_ids_all"].update(pkg["in_workload_ids_all"])
            view["source_pkgs"][srpm_id]["in_workload_ids_req"].update(pkg["in_workload_ids_req"])
            view["source_pkgs"][srpm_id]["in_workload_ids_dep"].update(pkg["in_workload_ids_dep"])
            view["source_pkgs"][srpm_id]["in_workload_ids_env"].update(pkg["in_workload_ids_env"])
        
        log("  Includes {} source packages.".format(len(view["source_pkgs"])))


        log("  DONE!")
        log("")

        return view


    def _analyze_views(self):

        views = {}

        # First, analyse the standard views
        for view_conf_id in self.configs["views"]:
            view_conf = self.configs["views"][view_conf_id]

            if view_conf["type"] == "compose":
                for arch in view_conf["architectures"]:
                    view = self._analyze_view(view_conf, arch, views)
                    view_id = view["id"]

                    views[view_id] = view
        
        # Second, analyse the addon views
        # This is important as they need the standard views already available
        for view_conf_id in self.configs["views"]:
            view_conf = self.configs["views"][view_conf_id]

            if view_conf["type"] == "addon":
                base_view_conf_id = view_conf["base_view_id"]
                base_view_conf = self.configs["views"][base_view_conf_id]

                for arch in set(view_conf["architectures"]) & set(base_view_conf["architectures"]):
                    view = self._analyze_view(view_conf, arch, views)
                    view_id = view["id"]

                    views[view_id] = view
        
        self.data["views"] = views


    def _populate_buildroot_with_view_srpms(self, view_conf, arch):
        view_conf_id = view_conf["id"]

        log("Initialising buildroot packages of: {view_name} ({view_conf_id}) for {arch}".format(
            view_name=view_conf["name"],
            view_conf_id=view_conf_id,
            arch=arch
        ))

        view_id = "{view_conf_id}:{arch}".format(
            view_conf_id=view_conf_id,
            arch=arch
        )

        view = self.data["views"][view_id]
        repo_id = view_conf["repository"]

        # Initialise the srpms section
        if repo_id not in self.data["buildroot"]["srpms"]:
            self.data["buildroot"]["srpms"][repo_id] = {}

        if arch not in self.data["buildroot"]["srpms"][repo_id]:
            self.data["buildroot"]["srpms"][repo_id][arch] = {}

        # Initialise each srpm
        for srpm_id, srpm in view["source_pkgs"].items():

            if srpm["placeholder"]:
                directly_required_pkg_names = srpm["placeholder_directly_required_pkg_names"]
            
            else:
                # This is the same set in both koji_srpms and srpms
                directly_required_pkg_names = set()

                # Do I need to extract the build dependencies from koji root_logs?
                # Then also save the srpms in the koji_srpm section
                if view_conf["buildroot_strategy"] == "root_logs":
                    srpm_reponame = srpm["reponame"]
                    koji_api_url = self.configs["repos"][repo_id]["source"]["repos"][srpm_reponame]["koji_api_url"]
                    koji_files_url = self.configs["repos"][repo_id]["source"]["repos"][srpm_reponame]["koji_files_url"]
                    koji_id = url_to_id(koji_api_url)

                    # Initialise the koji_srpms section
                    if koji_id not in self.data["buildroot"]["koji_srpms"]:
                        # SRPMs
                        self.data["buildroot"]["koji_srpms"][koji_id] = {}
                        # URLs
                        self.data["buildroot"]["koji_urls"][koji_id] = {}
                        self.data["buildroot"]["koji_urls"][koji_id]["api"] = koji_api_url
                        self.data["buildroot"]["koji_urls"][koji_id]["files"] = koji_files_url
                    
                    if arch not in self.data["buildroot"]["koji_srpms"][koji_id]:
                        self.data["buildroot"]["koji_srpms"][koji_id][arch] = {}

                    # Initialise srpms in the koji_srpms section
                    if srpm_id not in self.data["buildroot"]["koji_srpms"][koji_id][arch]:
                        self.data["buildroot"]["koji_srpms"][koji_id][arch][srpm_id] = {}
                        self.data["buildroot"]["koji_srpms"][koji_id][arch][srpm_id]["id"] = srpm_id
                        self.data["buildroot"]["koji_srpms"][koji_id][arch][srpm_id]["directly_required_pkg_names"] = directly_required_pkg_names
                    else:
                        directly_required_pkg_names = self.data["buildroot"]["koji_srpms"][koji_id][arch][srpm_id]["directly_required_pkg_names"]

            # Initialise srpms in the srpms section
            if srpm_id not in self.data["buildroot"]["srpms"][repo_id][arch]:
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id] = {}
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["id"] = srpm_id
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["directly_required_pkg_names"] = directly_required_pkg_names
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["pkg_relations"] = {}
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["pkg_env_ids"] = set()
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["pkg_added_ids"] = set()
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["errors"] = {}
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["errors"]["non_existing_pkgs"] = set()
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["errors"]["message"] = ""
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["succeeded"] = False
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["queued"] = False
                self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["processed"] = False


        log("  DONE!")
        log("")


    def _resolve_srpms_using_root_logs_parallel(self, pass_counter):
        """
        This function is idempotent!
        """
        log("== Resolving SRPMs using root logs - pass {} (PARALLEL) ========".format(pass_counter))

        # Collect work items (skip cached entries)
        work_items = []
        total_srpms_to_resolve = 0

        for koji_id in self.data["buildroot"]["koji_srpms"]:
            koji_urls = self.data["buildroot"]["koji_urls"][koji_id]

            # If the cache is empty, initialise it
            if koji_id not in self.cache["root_log_deps"]["current"]:
                self.cache["root_log_deps"]["current"][koji_id] = {}
            if koji_id not in self.cache["root_log_deps"]["next"]:
                self.cache["root_log_deps"]["next"][koji_id] = {}

            for arch in self.data["buildroot"]["koji_srpms"][koji_id]:

                # If the cache is empty, initialise it
                if arch not in self.cache["root_log_deps"]["current"][koji_id]:
                    self.cache["root_log_deps"]["current"][koji_id][arch] = {}
                if arch not in self.cache["root_log_deps"]["next"][koji_id]:
                    self.cache["root_log_deps"]["next"][koji_id][arch] = {}

                for srpm_id, srpm in self.data["buildroot"]["koji_srpms"][koji_id][arch].items():
                    total_srpms_to_resolve += 1

                    # Skip if already processed or cached
                    if srpm["directly_required_pkg_names"]:
                        log(f"  Skipping {srpm_id} {arch} (already done before)")
                        continue

                    if srpm_id in self.cache["root_log_deps"]["current"][koji_id][arch]:
                        log(f"  Using Cache for {srpm_id} {arch}!")
                        directly_required_pkg_names = self.cache["root_log_deps"]["current"][koji_id][arch][srpm_id]
                        self.cache["root_log_deps"]["next"][koji_id][arch][srpm_id] = directly_required_pkg_names
                        srpm["directly_required_pkg_names"].update(directly_required_pkg_names)
                        continue

                    if srpm_id in self.cache["root_log_deps"]["next"][koji_id][arch]:
                        log(f"  Using Cache for {srpm_id} {arch}!")
                        directly_required_pkg_names = self.cache["root_log_deps"]["next"][koji_id][arch][srpm_id]
                        srpm["directly_required_pkg_names"].update(directly_required_pkg_names)
                        continue

                    # Add to work queue
                    work_items.append({
                        'koji_id': koji_id,
                        'koji_api_url': koji_urls["api"],
                        'koji_files_url': koji_urls["files"],
                        'srpm_id': srpm_id,
                        'arch': arch,
                        'dev_buildroot': self.settings.get("dev_buildroot", False)
                    })

        if not work_items:
            log("All SRPMs already cached or processed!")
            return

        log(f"Processing {len(work_items)} SRPMs in parallel (out of {total_srpms_to_resolve} total)")

        # Process in parallel using ProcessPoolExecutor
        max_workers = min(self.settings["parallel_max"], len(work_items))

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            future_to_item = {
                executor.submit(process_single_srpm_root_log, item): item
                for item in work_items
            }

            # Collect results as they complete
            completed_count = 0
            total_count = len(work_items)

            for future in as_completed(future_to_item):
                completed_count += 1
                work_item = future_to_item[future]

                try:
                    result = future.result()
                    self._apply_srpm_result(work_item, result)

                    if result['error']:
                        log(f"[ Buildroot - pass {pass_counter} - {completed_count} of {total_count} ] "
                            f"Failed {result['srpm_id']} {result['arch']}: {result['error']}")
                    else:
                        log(f"[ Buildroot - pass {pass_counter} - {completed_count} of {total_count} ] "
                            f"Completed {result['srpm_id']} {result['arch']} - found {len(result['deps'])} deps")

                except Exception as e:
                    log(f"Failed to process {work_item['srpm_id']}: {e}")
                    # Apply empty result for failed processing
                    error_result = {
                        'srpm_id': work_item['srpm_id'],
                        'arch': work_item['arch'],
                        'deps': [],
                        'error': str(e)
                    }
                    self._apply_srpm_result(work_item, error_result)

        # Save updated cache
        dump_data(self.settings["root_log_deps_cache_path"], self.cache["root_log_deps"]["next"])

        log("")
        log("  DONE!")
        log("")


    def _apply_srpm_result(self, work_item, result):
        """Apply worker result back to main data structures"""
        koji_id = work_item['koji_id']
        arch = work_item['arch']
        srpm_id = work_item['srpm_id']
        deps = result['deps']

        # Update cache
        self.cache["root_log_deps"]["next"][koji_id][arch][srpm_id] = deps

        # Update main data
        # Here it's important to add the packages to the already initiated
        # set, because its reference is shared between the koji_srpms and the srpm sections
        self.data["buildroot"]["koji_srpms"][koji_id][arch][srpm_id]["directly_required_pkg_names"].update(deps)


    def _analyze_build_groups(self):

        log("")
        log("Analyzing build groups...")
        log("")

        # Need to analyse build groups for all repo_ids
        # and arches of buildroot["srpms"]
        for repo_id in self.data["buildroot"]["srpms"]:
            self.data["buildroot"]["build_groups"][repo_id] = {}

            for arch in self.data["buildroot"]["srpms"][repo_id]:

                generated_id = "CR-buildroot-base-env-{repo_id}-{arch}".format(
                    repo_id=repo_id,
                    arch=arch
                )

                # Using the _analyze_env function! 
                # So I need to reconstruct a fake env_conf
                fake_env_conf = {}
                fake_env_conf["id"] = generated_id
                fake_env_conf["options"] = []
                if self.configs["repos"][repo_id]["source"]["base_buildroot_override"]:
                    fake_env_conf["packages"] = self.configs["repos"][repo_id]["source"]["base_buildroot_override"]
                    fake_env_conf["groups"] = []
                else:
                    fake_env_conf["packages"] = []
                    fake_env_conf["groups"] = ["build"]
                fake_env_conf["arch_packages"] = {}
                fake_env_conf["arch_packages"][arch] = []

                log("Resolving build group: {repo_id} {arch}".format(
                    repo_id=repo_id,
                    arch=arch
                ))
                repo = self.configs["repos"][repo_id]
                fake_env = self._analyze_env(fake_env_conf, repo, arch)

                # If this fails, the buildroot can't be resolved.
                # Fail the entire content resolver build!
                if not fake_env["succeeded"]:
                    raise BuildGroupAnalysisError

                self.data["buildroot"]["build_groups"][repo_id][arch] = fake_env
                self.data["buildroot"]["build_groups"][repo_id][arch]["generated_id"] = generated_id

        log("")
        log("  DONE!")
        log("")


    def _expand_buildroot_srpms(self):
        # This function is idempotent!
        # 
        # That means it can be run many times without affecting the old results.

        log("Expanding the SRPM set...")

        counter = 0

        for repo_id in self.data["buildroot"]["srpms"]:
            for arch in self.data["buildroot"]["srpms"][repo_id]:
                top_lvl_srpm_ids = set(self.data["buildroot"]["srpms"][repo_id][arch])
                for top_lvl_srpm_id in top_lvl_srpm_ids:
                    top_lvl_srpm = self.data["buildroot"]["srpms"][repo_id][arch][top_lvl_srpm_id]

                    for pkg_id in top_lvl_srpm["pkg_relations"]:
                        srpm_id = self.data["pkgs"][repo_id][arch][pkg_id]["sourcerpm"].rsplit(".src.rpm")[0]

                        if srpm_id in self.data["buildroot"]["srpms"][repo_id][arch]:
                            continue

                        # Adding a new one!
                        counter += 1
                        
                        srpm_reponame = self.data["pkgs"][repo_id][arch][pkg_id]["reponame"]

                        # This is the same set in both koji_srpms and srpms
                        directly_required_pkg_names = set()

                        koji_api_url = self.configs["repos"][repo_id]["source"]["repos"][srpm_reponame]["koji_api_url"]
                        koji_files_url = self.configs["repos"][repo_id]["source"]["repos"][srpm_reponame]["koji_files_url"]
                        koji_id = url_to_id(koji_api_url)

                        # Initialise the srpm in the koji_srpms section
                        if srpm_id not in self.data["buildroot"]["koji_srpms"][koji_id][arch]:
                            self.data["buildroot"]["koji_srpms"][koji_id][arch][srpm_id] = {}
                            self.data["buildroot"]["koji_srpms"][koji_id][arch][srpm_id]["id"] = srpm_id
                            self.data["buildroot"]["koji_srpms"][koji_id][arch][srpm_id]["directly_required_pkg_names"] = directly_required_pkg_names
                        else:
                            directly_required_pkg_names = self.data["buildroot"]["koji_srpms"][koji_id][arch][srpm_id]["directly_required_pkg_names"]

                        # Initialise the srpm in the srpms section
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id] = {}
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["id"] = srpm_id
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["directly_required_pkg_names"] = directly_required_pkg_names
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["pkg_relations"] = {}
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["pkg_env_ids"] = set()
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["pkg_added_ids"] = set()
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["errors"] = {}
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["errors"]["non_existing_pkgs"] = set()
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["errors"]["message"] = ""
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["succeeded"] = False
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["queued"] = False
                        self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["processed"] = False

        log("  Found {} new SRPMs!".format(counter))
        log("  DONE!")
        log("")

        return counter


    def _analyze_srpm_buildroots(self, pass_counter):
        # This function is idempotent!
        # 
        # That means it can be run many times without affecting the old results.

        log("")
        log("Analyzing SRPM buildroots...")
        log("")

        # Initialise things for the workload resolver
        self._reset_workload_processing_queue()
        fake_workload_results = {}

        # Prepare a counter for the log
        total_srpms_to_resolve = 0
        for repo_id in self.data["buildroot"]["srpms"]:
            for arch in self.data["buildroot"]["srpms"][repo_id]:
                for srpm_id, srpm in self.data["buildroot"]["srpms"][repo_id][arch].items():
                    if srpm["processed"]:
                        continue
                    total_srpms_to_resolve += 1
        srpms_to_resolve_counter = 0

        for repo_id in self.data["buildroot"]["srpms"]:
            for arch in self.data["buildroot"]["srpms"][repo_id]:
                for srpm_id, srpm in self.data["buildroot"]["srpms"][repo_id][arch].items():

                    if srpm["queued"] or srpm["processed"]:
                        continue

                    # Using the _analyze_workload function!
                    # So I need to reconstruct a fake workload_conf and a fake env_conf
                    fake_workload_conf = {}
                    fake_workload_conf["labels"] = []
                    fake_workload_conf["id"] = srpm_id
                    fake_workload_conf["options"] = []
                    fake_workload_conf["packages"] = srpm["directly_required_pkg_names"]
                    fake_workload_conf["groups"] = []
                    fake_workload_conf["package_placeholders"] = {}
                    fake_workload_conf["package_placeholders"]["pkgs"] = {}
                    fake_workload_conf["package_placeholders"]["srpms"] = {}
                    fake_workload_conf["arch_packages"] = {}
                    fake_workload_conf["arch_packages"][arch] = []

                    fake_env_conf = {}
                    fake_env_conf["labels"] = []
                    fake_env_conf["id"] = self.data["buildroot"]["build_groups"][repo_id][arch]["generated_id"]
                    fake_env_conf["packages"] = ["bash"] # This just needs to pass the "if len(packages)" test as True
                    fake_env_conf["arch_packages"] = {}
                    fake_env_conf["arch_packages"][arch] = []

                    srpms_to_resolve_counter += 1
                    
                    #log("[ Buildroot - pass {} - {} of {} ]".format(pass_counter, srpms_to_resolve_counter, total_srpms_to_resolve))
                    #log("Resolving SRPM buildroot: {repo_id} {arch} {srpm_id}".format(
                    #    repo_id=repo_id,
                    #    arch=arch,
                    #    srpm_id=srpm_id
                    #))
                    repo = self.configs["repos"][repo_id]

                    #fake_workload = self._analyze_workload(fake_workload_conf, fake_env_conf, repo, arch)
                    self._queue_workload_processing(fake_workload_conf, fake_env_conf, repo, arch)

                    # Save the buildroot data
                    self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["queued"] = True

        asyncio.run(self._analyze_workloads_async(fake_workload_results))

        for repo_id in self.data["buildroot"]["srpms"]:
            for arch in self.data["buildroot"]["srpms"][repo_id]:
                for srpm_id, srpm in self.data["buildroot"]["srpms"][repo_id][arch].items():

                    if srpm["processed"]:
                        continue

                    fake_workload_id = "{workload_conf_id}:{env_conf_id}:{repo_id}:{arch}".format(
                        workload_conf_id=srpm_id,
                        env_conf_id=self.data["buildroot"]["build_groups"][repo_id][arch]["generated_id"],
                        repo_id=repo_id,
                        arch=arch
                    )

                    fake_workload = fake_workload_results[fake_workload_id]

                    # Save the buildroot data
                    self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["succeeded"] = fake_workload["succeeded"]
                    self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["pkg_relations"] = fake_workload["pkg_relations"]
                    self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["pkg_env_ids"] = fake_workload["pkg_env_ids"]
                    self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["pkg_added_ids"] = fake_workload["pkg_added_ids"]
                    self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["errors"] = fake_workload["errors"]
                    self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["warnings"] = fake_workload["warnings"]
                    self.data["buildroot"]["srpms"][repo_id][arch][srpm_id]["processed"] = True

        log("")
        log("  DONE!")
        log("")


    def _analyze_buildroot(self):

        self._global_performance_hack_run_recommends_queries = False

        self._record_metric("started _analyze_buildroot()")

        self.data["buildroot"] = {}
        self.data["buildroot"]["koji_srpms"] = {}
        self.data["buildroot"]["koji_urls"] = {}
        self.data["buildroot"]["srpms"] = {}
        self.data["buildroot"]["build_groups"] = {}

        # Currently, only "compose" view types are supported.
        # The "addon" type is not.

        # Get SRPMs from views
        #
        # This populates:
        #   data["buildroot"]["koji_srpms"]...
        # and also initiates:
        #   data["buildroot"]["srpms"]...
        for view_conf_id in self.configs["views"]:
            view_conf = self.configs["views"][view_conf_id]

            if view_conf["type"] == "compose":
                if view_conf["buildroot_strategy"] == "root_logs":
                    for arch in view_conf["architectures"]:
                        self._populate_buildroot_with_view_srpms(view_conf, arch)

        # Time to resolve the build groups!
        # 
        # This initialises and populates:
        #   buildroot["build_groups"]
        self._analyze_build_groups()

        pass_counter = 0
        while True:
            pass_counter += 1

            self._record_metric("  started pass {}:".format(pass_counter))

            log("")
            log("== Buildroot resolution - pass {} ========".format(pass_counter))
            log("")
            log("")
            # Get the directly_required_pkg_names from koji root logs
            # 
            # Adds stuff to existing:
            #   data["buildroot"]["koji_srpms"]...
            # ... which also updates:
            #   data["buildroot"]["srpms"]...
            # ... because it's interlinked.
            self._resolve_srpms_using_root_logs_parallel(pass_counter)

            self._record_metric("    finished _resolve_srpms_using_root_logs")

            # And now resolving the actual buildroot
            self._analyze_srpm_buildroots(pass_counter)

            self._record_metric("    finished _analyze_srpm_buildroots")

            # Resolving dependencies could have added new SRPMs into the mix that also
            # need their buildroots resolved! So let's find out if there are any
            new_srpms_count = self._expand_buildroot_srpms()

            self._record_metric("    finished with new_srpms_count == {}".format(new_srpms_count))

            if not new_srpms_count:
                log("")
                log("All passes completed!")
                log("")
                break


    def _add_missing_levels_to_pkg_or_srpm(self, pkg_or_srpm, level):

        pkg_current_max_level = len(pkg_or_srpm["level"]) - 1
        for _ in range(level - pkg_current_max_level):
            pkg_or_srpm["level"].append({
                "all": set(),
                "req": set(),
                "dep": set(),
                "env": set()
            })


    def _add_buildroot_to_view(self, view_conf, arch):

        view_conf_id = view_conf["id"]

        view_id = "{view_conf_id}:{arch}".format(
            view_conf_id=view_conf_id,
            arch=arch
        )

        repo_id = view_conf["repository"]

        view = self.data["views"][view_id]


        log("")
        log("Adding buildroot to view {}...".format(view_id))

        # Starting with all SRPMs in this view
        srpm_ids_to_process = set(view["source_pkgs"])

        # Starting on level 1, the first buildroot level
        # (it's 0 because it gets incremented to 1 immediately after the loop starts)
        level = 0

        while True:
            level += 1
            added_pkg_ids = set()

            log("  Pass {}...".format(level))

            # This is similar to adding workloads in _analyze_view()
            for buildroot_srpm_id in srpm_ids_to_process:
                buildroot_srpm = self.data["buildroot"]["srpms"][repo_id][arch][buildroot_srpm_id]


                # Packages in the base buildroot (which would be the environment in workloads)
                for pkg_id in buildroot_srpm["pkg_env_ids"]:
                    added_pkg_ids.add(pkg_id)

                    # Initialise
                    if pkg_id not in view["pkgs"]:
                        pkg = self.data["pkgs"][repo_id][arch][pkg_id]
                        view["pkgs"][pkg_id] = self._init_view_pkg(pkg, arch, level=level)
                    
                    # Add missing levels to the pkg
                    self._add_missing_levels_to_pkg_or_srpm(view["pkgs"][pkg_id], level)

                    # It's in this buildroot
                    view["pkgs"][pkg_id]["in_buildroot_of_srpm_id_all"].add(buildroot_srpm_id)
                    view["pkgs"][pkg_id]["level"][level]["all"].add(buildroot_srpm_id)

                    # And in the base buildroot specifically
                    view["pkgs"][pkg_id]["in_buildroot_of_srpm_id_env"].add(buildroot_srpm_id)
                    view["pkgs"][pkg_id]["level"][level]["env"].add(buildroot_srpm_id)

                    # Is it also required?
                    if view["pkgs"][pkg_id]["name"] in buildroot_srpm["directly_required_pkg_names"]:
                        view["pkgs"][pkg_id]["in_buildroot_of_srpm_id_req"].add(buildroot_srpm_id)
                        view["pkgs"][pkg_id]["level"][level]["req"].add(buildroot_srpm_id)
                    
                    # pkg_relations
                    view["pkgs"][pkg_id]["required_by"].update(buildroot_srpm["pkg_relations"][pkg_id]["required_by"])
                    view["pkgs"][pkg_id]["recommended_by"].update(buildroot_srpm["pkg_relations"][pkg_id]["recommended_by"])
                    view["pkgs"][pkg_id]["suggested_by"].update(buildroot_srpm["pkg_relations"][pkg_id]["suggested_by"])
                    view["pkgs"][pkg_id]["supplements"].update(buildroot_srpm["pkg_relations"][pkg_id]["supplements"])

                # Packages needed on top of the base buildroot (required or dependency)
                for pkg_id in buildroot_srpm["pkg_added_ids"]:
                    added_pkg_ids.add(pkg_id)

                    # Initialise
                    if pkg_id not in view["pkgs"]:
                        pkg = self.data["pkgs"][repo_id][arch][pkg_id]
                        view["pkgs"][pkg_id] = self._init_view_pkg(pkg, arch, level=level)
                    
                    # Add missing levels to the pkg
                    self._add_missing_levels_to_pkg_or_srpm(view["pkgs"][pkg_id], level)
                    
                    # It's in this buildroot
                    view["pkgs"][pkg_id]["in_buildroot_of_srpm_id_all"].add(buildroot_srpm_id)
                    view["pkgs"][pkg_id]["level"][level]["all"].add(buildroot_srpm_id)

                    # Is it also required?
                    if view["pkgs"][pkg_id]["name"] in buildroot_srpm["directly_required_pkg_names"]:
                        view["pkgs"][pkg_id]["in_buildroot_of_srpm_id_req"].add(buildroot_srpm_id)
                        view["pkgs"][pkg_id]["level"][level]["req"].add(buildroot_srpm_id)
                    
                    # Or a dependency?
                    else:
                        view["pkgs"][pkg_id]["in_buildroot_of_srpm_id_dep"].add(buildroot_srpm_id)
                        view["pkgs"][pkg_id]["level"][level]["dep"].add(buildroot_srpm_id)
                    
                    # pkg_relations
                    view["pkgs"][pkg_id]["required_by"].update(buildroot_srpm["pkg_relations"][pkg_id]["required_by"])
                    view["pkgs"][pkg_id]["recommended_by"].update(buildroot_srpm["pkg_relations"][pkg_id]["recommended_by"])
                    view["pkgs"][pkg_id]["suggested_by"].update(buildroot_srpm["pkg_relations"][pkg_id]["suggested_by"])
                    view["pkgs"][pkg_id]["supplements"].update(buildroot_srpm["pkg_relations"][pkg_id]["supplements"])
            
            # Resetting the SRPMs, so only the new ones can be added
            srpm_ids_to_process = set()

            # SRPMs
            for pkg_id in added_pkg_ids:
                pkg = view["pkgs"][pkg_id]
                srpm_id = pkg["sourcerpm"].rsplit(".src.rpm")[0]

                # Initialise
                if srpm_id not in view["source_pkgs"]:
                    view["source_pkgs"][srpm_id] = self._init_view_srpm(pkg, level=level)
                    srpm_ids_to_process.add(srpm_id)
                    
                # Add missing levels to the pkg
                self._add_missing_levels_to_pkg_or_srpm(view["source_pkgs"][srpm_id], level)

                view["source_pkgs"][srpm_id]["pkg_ids"].add(pkg_id)

                # Include some information from the RPM
                view["source_pkgs"][srpm_id]["in_buildroot_of_srpm_id_all"].update(pkg["in_buildroot_of_srpm_id_all"])
                view["source_pkgs"][srpm_id]["in_buildroot_of_srpm_id_req"].update(pkg["in_buildroot_of_srpm_id_req"])
                view["source_pkgs"][srpm_id]["in_buildroot_of_srpm_id_dep"].update(pkg["in_buildroot_of_srpm_id_dep"])
                view["source_pkgs"][srpm_id]["in_buildroot_of_srpm_id_env"].update(pkg["in_buildroot_of_srpm_id_env"])
                view["source_pkgs"][srpm_id]["level"][level]["all"].update(pkg["level"][level]["all"])
                view["source_pkgs"][srpm_id]["level"][level]["req"].update(pkg["level"][level]["req"])
                view["source_pkgs"][srpm_id]["level"][level]["dep"].update(pkg["level"][level]["dep"])
                view["source_pkgs"][srpm_id]["level"][level]["env"].update(pkg["level"][level]["env"])

            log ("    added {} RPMs".format(len(added_pkg_ids)))
            log ("    added {} SRPMs".format(len(srpm_ids_to_process)))

            # More iterations needed?
            if not srpm_ids_to_process:
                log("  All passes completed!")
                log("")
                break


    def _add_buildroot_to_views(self):

        log("")
        log("Adding Buildroot to views...")
        log("")

        # First, the standard views
        for view_conf_id in self.configs["views"]:
            view_conf = self.configs["views"][view_conf_id]

            if view_conf["type"] == "compose":
                if view_conf["buildroot_strategy"] == "root_logs":
                    for arch in view_conf["architectures"]:
                        self._add_buildroot_to_view(view_conf, arch)

        # And the addon is not supported now

        log("")
        log("  DONE!")
        log("")


    def _init_pkg_or_srpm_relations_fields(self, target_pkg, type = None):
        # I kept them all listed so they're easy to copy

        # Workload IDs
        target_pkg["in_workload_ids_all"] = set()
        target_pkg["in_workload_ids_req"] = set()
        target_pkg["in_workload_ids_dep"] = set()
        target_pkg["in_workload_ids_env"] = set()
        
        # Workload Conf IDs
        target_pkg["in_workload_conf_ids_all"] = set()
        target_pkg["in_workload_conf_ids_req"] = set()
        target_pkg["in_workload_conf_ids_dep"] = set()
        target_pkg["in_workload_conf_ids_env"] = set()

        # Buildroot SRPM IDs
        target_pkg["in_buildroot_of_srpm_id_all"] = set()
        target_pkg["in_buildroot_of_srpm_id_req"] = set()
        target_pkg["in_buildroot_of_srpm_id_dep"] = set()
        target_pkg["in_buildroot_of_srpm_id_env"] = set()

        # Buildroot SRPM Names
        target_pkg["in_buildroot_of_srpm_name_all"] = {} # of set() of srpm_ids
        target_pkg["in_buildroot_of_srpm_name_req"] = {} # of set() of srpm_ids
        target_pkg["in_buildroot_of_srpm_name_dep"] = {} # of set() of srpm_ids
        target_pkg["in_buildroot_of_srpm_name_env"] = {} # of set() of srpm_ids

        # Unwanted
        target_pkg["unwanted_completely_in_list_ids"] = set()
        target_pkg["unwanted_buildroot_in_list_ids"] = set()

        # Level number
        target_pkg["level_number"] = 999

        # Levels
        target_pkg["level"] = []

        # Maintainer recommendation
        target_pkg["maintainer_recommendation"] = {}
        target_pkg["maintainer_recommendation_details"] = {}
        target_pkg["best_maintainers"] = set()

        if type == "rpm":

            # Dependency of RPM NEVRs
            target_pkg["dependency_of_pkg_nevrs"] = set()
            target_pkg["hard_dependency_of_pkg_nevrs"] = set()
            target_pkg["weak_dependency_of_pkg_nevrs"] = set()
            target_pkg["reverse_weak_dependency_of_pkg_nevrs"] = set()


            # Dependency of RPM Names
            target_pkg["dependency_of_pkg_names"] = {} # of set() of nevrs
            target_pkg["hard_dependency_of_pkg_names"] = {} # of set() of nevrs
            target_pkg["weak_dependency_of_pkg_names"] = {} # if set() of nevrs
            target_pkg["reverse_weak_dependency_of_pkg_names"] = {} # if set() of nevrs
    
    _RELATION_LIST_TYPES = ["all", "req", "dep", "env"]
    _WORKLOAD_ID_KEYS = ["in_workload_ids_all", "in_workload_ids_req", "in_workload_ids_dep", "in_workload_ids_env"]
    _WORKLOAD_CONF_KEYS = ["in_workload_conf_ids_all", "in_workload_conf_ids_req", "in_workload_conf_ids_dep", "in_workload_conf_ids_env"]
    _BUILDROOT_ID_KEYS = ["in_buildroot_of_srpm_id_all", "in_buildroot_of_srpm_id_req", "in_buildroot_of_srpm_id_dep", "in_buildroot_of_srpm_id_env"]
    _BUILDROOT_NAME_KEYS = ["in_buildroot_of_srpm_name_all", "in_buildroot_of_srpm_name_req", "in_buildroot_of_srpm_name_dep", "in_buildroot_of_srpm_name_env"]

    def _populate_pkg_or_srpm_relations_fields(self, target_pkg, source_pkg, type = None, view = None):

        if type == "rpm" and not view:
            raise ValueError("This function requires a view when using type = 'rpm'!")

        target_pkg["unwanted_completely_in_list_ids"].update(source_pkg["unwanted_completely_in_list_ids"])
        target_pkg["unwanted_buildroot_in_list_ids"].update(source_pkg["unwanted_buildroot_in_list_ids"])

        for i in range(4):
            wid_key = self._WORKLOAD_ID_KEYS[i]
            wcid_key = self._WORKLOAD_CONF_KEYS[i]
            bid_key = self._BUILDROOT_ID_KEYS[i]
            bname_key = self._BUILDROOT_NAME_KEYS[i]

            src_wids = source_pkg[wid_key]
            target_pkg[wid_key].update(src_wids)

            src_bids = source_pkg[bid_key]
            target_pkg[bid_key].update(src_bids)

            target_wcids = target_pkg[wcid_key]
            for workload_id in src_wids:
                target_wcids.add(workload_id.split(":")[0])

            target_bnames = target_pkg[bname_key]
            for srpm_id in src_bids:
                srpm_name = srpm_id.rsplit("-", 2)[0]
                if srpm_name not in target_bnames:
                    target_bnames[srpm_name] = set()
                target_bnames[srpm_name].add(srpm_id)
        
        # Level number
        level_number = 0
        for level in source_pkg["level"]:
            if level["all"]:
                if level_number < target_pkg["level_number"]:
                    target_pkg["level_number"] = level_number
            level_number += 1

        # All the levels!
        level = 0
        for level_data in source_pkg["level"]:
            # 'level' is the number
            # 'level_data' is the ["all"][workload_id] or ["all"][srpm_id] or
            #                     ["req"][workload_id] or ["req"][srpm_id] or 
            #                     ["dep"][workload_id] or ["dep"][srpm_id] or
            #                     ["env"][workload_id] or ["env"][srpm_id]

            # If I could do 'if level in target_pkg["level"]' I'd do that instead...
            # But it's a list, so have to do this instead
            if len(target_pkg["level"]) == level:
                target_pkg["level"].append(dict())

            for level_scope, those_ids in level_data.items():
                # 'level_scope' is "all" or "req" etc.
                # 'those_ids' is a list of srpm_ids or workload_ids

                if level_scope not in target_pkg["level"][level]:
                    target_pkg["level"][level][level_scope] = set()
                
                target_pkg["level"][level][level_scope].update(those_ids)
            
            level +=1
 
        
        if type == "rpm":
            # Hard dependency of
            for pkg_id in source_pkg["required_by"]:
                pkg_name = pkg_id_to_name(pkg_id)
                
                # This only happens in addon views, and only rarely.
                # Basically means that a package in the addon view is required
                # by a package in the base view.
                # Doesn't make sense?
                # Think of 'glibc-all-langpacks' being in the addon,
                # while the proper langpacks along with 'glibc' are in the base view.
                # 
                # In that case, 'glibc' is not in the addon, but 'glibc-all-langpacks'
                # requires it.
                #
                # I'm not implementing it now, as it's such a corner case.
                # So just skip it. All the data will remain correct,
                # it's just the 'glibc-all-langpacks' page won't show
                # "required by 'glibc'" that's all.
                if pkg_id not in view["pkgs"]:
                    view_conf_id = view["view_conf_id"]
                    view_conf = self.configs["views"][view_conf_id]
                    if view_conf["type"] == "addon":
                        continue

                pkg = view["pkgs"][pkg_id]
                pkg_nevr = "{name}-{evr}".format(
                    name=pkg["name"],
                    evr=pkg["evr"]
                )
                target_pkg["hard_dependency_of_pkg_nevrs"].add(pkg_nevr)

                if pkg_name not in target_pkg["hard_dependency_of_pkg_names"]:
                    target_pkg["hard_dependency_of_pkg_names"][pkg_name] = set()
                target_pkg["hard_dependency_of_pkg_names"][pkg_name].add(pkg_nevr)

            # Weak dependency of
            for list_type in ["recommended", "suggested"]:
                for pkg_id in source_pkg["{}_by".format(list_type)]:
                    pkg_name = pkg_id_to_name(pkg_id)

                    # This only happens in addon views, and only rarely.
                    # (see the long comment above)
                    if pkg_id not in view["pkgs"]:
                        view_conf_id = view["view_conf_id"]
                        view_conf = self.configs["views"][view_conf_id]
                        if view_conf["type"] == "addon":
                            continue

                    pkg = view["pkgs"][pkg_id]
                    pkg_nevr = "{name}-{evr}".format(
                        name=pkg["name"],
                        evr=pkg["evr"]
                    )
                    target_pkg["weak_dependency_of_pkg_nevrs"].add(pkg_nevr)

                    if pkg_name not in target_pkg["weak_dependency_of_pkg_names"]:
                        target_pkg["weak_dependency_of_pkg_names"][pkg_name] = set()
                    target_pkg["weak_dependency_of_pkg_names"][pkg_name].add(pkg_nevr)

            # Reverse weak dependency of (supplements)
            for pkg_id in source_pkg["supplements"]:
                pkg_name = pkg_id_to_name(pkg_id)

                # This only happens in addon views, and only rarely.
                # (see the long comment above)
                if pkg_id not in view["pkgs"]:
                    view_conf_id = view["view_conf_id"]
                    view_conf = self.configs["views"][view_conf_id]
                    if view_conf["type"] == "addon":
                        continue

                pkg = view["pkgs"][pkg_id]
                pkg_nevr = "{name}-{evr}".format(
                    name=pkg["name"],
                    evr=pkg["evr"]
                )
                target_pkg["reverse_weak_dependency_of_pkg_nevrs"].add(pkg_nevr)

                if pkg_name not in target_pkg["reverse_weak_dependency_of_pkg_names"]:
                    target_pkg["reverse_weak_dependency_of_pkg_names"][pkg_name] = set()
                target_pkg["reverse_weak_dependency_of_pkg_names"][pkg_name].add(pkg_nevr)
            
            # All types of dependency
            target_pkg["dependency_of_pkg_nevrs"].update(target_pkg["hard_dependency_of_pkg_nevrs"])
            target_pkg["dependency_of_pkg_nevrs"].update(target_pkg["weak_dependency_of_pkg_nevrs"])
            target_pkg["dependency_of_pkg_nevrs"].update(target_pkg["reverse_weak_dependency_of_pkg_nevrs"])

            for pkg_name, pkg_nevrs in target_pkg["hard_dependency_of_pkg_names"].items():
                if pkg_name not in target_pkg["dependency_of_pkg_names"]:
                    target_pkg["dependency_of_pkg_names"][pkg_name] = set()
                
                target_pkg["dependency_of_pkg_names"][pkg_name].update(pkg_nevrs)

            for pkg_name, pkg_nevrs in target_pkg["weak_dependency_of_pkg_names"].items():
                if pkg_name not in target_pkg["dependency_of_pkg_names"]:
                    target_pkg["dependency_of_pkg_names"][pkg_name] = set()
                
                target_pkg["dependency_of_pkg_names"][pkg_name].update(pkg_nevrs)

            for pkg_name, pkg_nevrs in target_pkg["reverse_weak_dependency_of_pkg_names"].items():
                if pkg_name not in target_pkg["dependency_of_pkg_names"]:
                    target_pkg["dependency_of_pkg_names"][pkg_name] = set()

                target_pkg["dependency_of_pkg_names"][pkg_name].update(pkg_nevrs)
            

        # TODO: add the levels


    def _generate_views_all_arches(self):

        views_all_arches = {}

        for view_conf_id, view_conf in self.configs["views"].items():

            #if view_conf["type"] == "compose":
            if True:

                repo_id = view_conf["repository"]

                view_all_arches = {}

                view_all_arches["id"] = view_conf_id
                view_all_arches["has_buildroot"] = False

                if view_conf["type"] == "compose":
                    if view_conf["buildroot_strategy"] == "root_logs":
                        view_all_arches["has_buildroot"] = True
                else:
                    view_all_arches["has_buildroot"] = False

                view_all_arches["everything_succeeded"] = True
                view_all_arches["no_warnings"] = True

                view_all_arches["workloads"] = {}

                view_all_arches["pkgs_by_name"] = {}
                view_all_arches["pkgs_by_nevr"] = {}

                view_all_arches["source_pkgs_by_name"] = {}

                view_all_arches["numbers"] = {}
                view_all_arches["numbers"]["pkgs"] = {}
                view_all_arches["numbers"]["pkgs"]["runtime"] = 0
                view_all_arches["numbers"]["pkgs"]["env"] = 0
                view_all_arches["numbers"]["pkgs"]["req"] = 0
                view_all_arches["numbers"]["pkgs"]["dep"] = 0
                view_all_arches["numbers"]["pkgs"]["build"] = 0
                view_all_arches["numbers"]["pkgs"]["build_base"] = 0
                view_all_arches["numbers"]["pkgs"]["build_level_1"] = 0
                view_all_arches["numbers"]["pkgs"]["build_level_2_plus"] = 0
                view_all_arches["numbers"]["srpms"] = {}
                view_all_arches["numbers"]["srpms"]["runtime"] = 0
                view_all_arches["numbers"]["srpms"]["env"] = 0
                view_all_arches["numbers"]["srpms"]["req"] = 0
                view_all_arches["numbers"]["srpms"]["dep"] = 0
                view_all_arches["numbers"]["srpms"]["build"] = 0
                view_all_arches["numbers"]["srpms"]["build_base"] = 0
                view_all_arches["numbers"]["srpms"]["build_level_1"] = 0
                view_all_arches["numbers"]["srpms"]["build_level_2_plus"] = 0


                for arch in view_conf["architectures"]:
                    view_id = "{view_conf_id}:{arch}".format(
                        view_conf_id=view_conf_id,
                        arch=arch
                    )

                    view = self.data["views"][view_id]

                    # Workloads
                    for workload_id in view["workload_ids"]:
                        workload = self.data["workloads"][workload_id]
                        workload_conf_id = workload["workload_conf_id"]
                        workload_conf = self.configs["workloads"][workload_conf_id]

                        if workload_conf_id not in view_all_arches["workloads"]:
                            view_all_arches["workloads"][workload_conf_id] = {}
                            view_all_arches["workloads"][workload_conf_id]["workload_conf_id"] = workload_conf_id
                            view_all_arches["workloads"][workload_conf_id]["name"] = workload_conf["name"]
                            view_all_arches["workloads"][workload_conf_id]["maintainer"] = workload_conf["maintainer"]
                            view_all_arches["workloads"][workload_conf_id]["succeeded"] = True
                            view_all_arches["workloads"][workload_conf_id]["no_warnings"] = True
                            # ...
                        
                        if not workload["succeeded"]:
                            view_all_arches["workloads"][workload_conf_id]["succeeded"] = False
                            view_all_arches["everything_succeeded"] = False
                        
                        if workload["warnings"]["message"]:
                            view_all_arches["workloads"][workload_conf_id]["no_warnings"] = False
                            view_all_arches["no_warnings"] = False


                    # Binary Packages
                    for package in view["pkgs"].values():

                        # Binary Packages by name
                        key = "pkgs_by_name"
                        identifier = package["name"]

                        # Init
                        if identifier not in view_all_arches[key]:
                            view_all_arches[key][identifier] = {}
                            view_all_arches[key][identifier]["name"] = package["name"]
                            view_all_arches[key][identifier]["placeholder"] = package["placeholder"]
                            view_all_arches[key][identifier]["source_name"] = package["source_name"]
                            view_all_arches[key][identifier]["nevrs"] = {}
                            view_all_arches[key][identifier]["arches"] = set()
                            view_all_arches[key][identifier]["highest_priority_reponames_per_arch"] = {}

                            self._init_pkg_or_srpm_relations_fields(view_all_arches[key][identifier], type="rpm")

                        if package["nevr"] not in view_all_arches[key][identifier]["nevrs"]:
                            view_all_arches[key][identifier]["nevrs"][package["nevr"]] = set()
                        view_all_arches[key][identifier]["nevrs"][package["nevr"]].add(arch)

                        view_all_arches[key][identifier]["arches"].add(arch)

                        if arch not in view_all_arches[key][identifier]["highest_priority_reponames_per_arch"]:
                            view_all_arches[key][identifier]["highest_priority_reponames_per_arch"][arch] = set()
                        view_all_arches[key][identifier]["highest_priority_reponames_per_arch"][arch].update(package["highest_priority_reponames"])

                        self._populate_pkg_or_srpm_relations_fields(view_all_arches[key][identifier], package, type="rpm", view=view)

                        # Binary Packages by nevr
                        key = "pkgs_by_nevr"
                        identifier = package["nevr"]

                        if identifier not in view_all_arches[key]:
                            view_all_arches[key][identifier] = {}
                            view_all_arches[key][identifier]["name"] = package["name"]
                            view_all_arches[key][identifier]["placeholder"] = package["placeholder"]
                            view_all_arches[key][identifier]["evr"] = package["evr"]
                            view_all_arches[key][identifier]["source_name"] = package["source_name"]
                            view_all_arches[key][identifier]["arches"] = set()
                            view_all_arches[key][identifier]["arches_arches"] = {}
                            view_all_arches[key][identifier]["reponame_per_arch"] = {}
                            view_all_arches[key][identifier]["highest_priority_reponames_per_arch"] = {}
                            view_all_arches[key][identifier]["category"] = None

                            self._init_pkg_or_srpm_relations_fields(view_all_arches[key][identifier], type="rpm")
                        
                        view_all_arches[key][identifier]["arches"].add(arch)
                        view_all_arches[key][identifier]["reponame_per_arch"][arch] = package["reponame"]
                        view_all_arches[key][identifier]["highest_priority_reponames_per_arch"][arch] = package["highest_priority_reponames"]

                        if arch not in view_all_arches[key][identifier]["arches_arches"]:
                            view_all_arches[key][identifier]["arches_arches"][arch] = set()
                        view_all_arches[key][identifier]["arches_arches"][arch].add(package["arch"])

                        self._populate_pkg_or_srpm_relations_fields(view_all_arches[key][identifier], package, type="rpm", view=view)

                    
                    # Source Packages
                    for package in view["source_pkgs"].values():

                        # Source Packages by name
                        key = "source_pkgs_by_name"
                        identifier = package["name"]

                        if identifier not in view_all_arches[key]:
                            view_all_arches[key][identifier] = {}
                            view_all_arches[key][identifier]["name"] = package["name"]
                            view_all_arches[key][identifier]["placeholder"] = package["placeholder"]
                            if view_all_arches["has_buildroot"]:
                                view_all_arches[key][identifier]["buildroot_succeeded"] = True
                                view_all_arches[key][identifier]["buildroot_no_warnings"] = True
                            view_all_arches[key][identifier]["errors"] = {}
                            view_all_arches[key][identifier]["warnings"] = {}
                            view_all_arches[key][identifier]["pkg_names"] = set()
                            view_all_arches[key][identifier]["pkg_nevrs"] = set()
                            view_all_arches[key][identifier]["arches"] = set()
                            view_all_arches[key][identifier]["category"] = None

                            self._init_pkg_or_srpm_relations_fields(view_all_arches[key][identifier])
                        

                        if view_all_arches["has_buildroot"]:
                            if not self.data["buildroot"]["srpms"][repo_id][arch][package["id"]]["succeeded"]:
                                view_all_arches["everything_succeeded"] = False
                                view_all_arches[key][identifier]["buildroot_succeeded"] = False
                                view_all_arches[key][identifier]["errors"][arch] = self.data["buildroot"]["srpms"][repo_id][arch][package["id"]]["errors"]
                            if self.data["buildroot"]["srpms"][repo_id][arch][package["id"]]["warnings"]["message"]:
                                view_all_arches["no_warnings"] = False
                                view_all_arches[key][identifier]["buildroot_no_warnings"] = False
                                view_all_arches[key][identifier]["warnings"][arch] = self.data["buildroot"]["srpms"][repo_id][arch][package["id"]]["warnings"]

                            
                        view_all_arches[key][identifier]["arches"].add(arch)

                        self._populate_pkg_or_srpm_relations_fields(view_all_arches[key][identifier], package, type="srpm")
                    

                    # Add binary packages to source packages
                    for pkg_id, pkg in view["pkgs"].items():

                        source_name = pkg["source_name"]

                        # Add package names
                        view_all_arches["source_pkgs_by_name"][source_name]["pkg_names"].add(pkg["name"])

                        # Add package nevrs
                        pkg_nevr = "{name}-{evr}".format(
                            name=pkg["name"],
                            evr=pkg["evr"]
                        )
                        view_all_arches["source_pkgs_by_name"][source_name]["pkg_nevrs"].add(pkg_nevr)
                                            
                

                # RPMs
                for pkg in view_all_arches["pkgs_by_nevr"].values():
                    category = None
                    if pkg["in_workload_ids_env"]:
                        category = "env"
                    elif pkg["in_workload_ids_req"]:
                        category = "req"
                    elif pkg["in_workload_ids_dep"]:
                        category = "dep"
                    elif pkg["in_buildroot_of_srpm_id_env"]:
                        category = "build_base"
                    elif pkg["in_buildroot_of_srpm_id_req"] or pkg["in_buildroot_of_srpm_id_dep"]:
                        if pkg["level_number"] == 1:
                            category = "build_level_1"
                        elif pkg["level_number"] > 1:
                            category = "build_level_2_plus"
                    
                    view_all_arches["numbers"]["pkgs"][category] += 1
                
                view_all_arches["numbers"]["pkgs"]["runtime"] = view_all_arches["numbers"]["pkgs"]["env"] + view_all_arches["numbers"]["pkgs"]["req"] + view_all_arches["numbers"]["pkgs"]["dep"]
                view_all_arches["numbers"]["pkgs"]["build"] = view_all_arches["numbers"]["pkgs"]["build_base"] + view_all_arches["numbers"]["pkgs"]["build_level_1"] + view_all_arches["numbers"]["pkgs"]["build_level_2_plus"]
                
                # SRPMs
                for pkg in view_all_arches["source_pkgs_by_name"].values():
                    category = None
                    if pkg["in_workload_ids_env"]:
                        category = "env"
                    elif pkg["in_workload_ids_req"]:
                        category = "req"
                    elif pkg["in_workload_ids_dep"]:
                        category = "dep"
                    elif pkg["in_buildroot_of_srpm_id_env"]:
                        category = "build_base"
                    elif pkg["in_buildroot_of_srpm_id_req"] or pkg["in_buildroot_of_srpm_id_dep"]:
                        if pkg["level_number"] == 1:
                            category = "build_level_1"
                        elif pkg["level_number"] > 1:
                            category = "build_level_2_plus"
                    
                    view_all_arches["numbers"]["srpms"][category] += 1
                
                view_all_arches["numbers"]["srpms"]["runtime"] = \
                    view_all_arches["numbers"]["srpms"]["env"] + \
                    view_all_arches["numbers"]["srpms"]["req"] + \
                    view_all_arches["numbers"]["srpms"]["dep"]

                view_all_arches["numbers"]["srpms"]["build"] = \
                    view_all_arches["numbers"]["srpms"]["build_base"] + \
                    view_all_arches["numbers"]["srpms"]["build_level_1"] + \
                    view_all_arches["numbers"]["srpms"]["build_level_2_plus"]





                # Done
                views_all_arches[view_conf_id] = view_all_arches
        
        self.data["views_all_arches"] = views_all_arches


    def _add_unwanted_packages_to_view(self, view, view_conf):

        arch = view["arch"]

        # Find exclusion lists mathing this view's label(s)
        unwanted_conf_ids = set()
        for view_label in view_conf["labels"]:
            for unwanted_conf_id, unwanted in self.configs["unwanteds"].items():
                for unwanted_label in unwanted["labels"]:
                    if view_label == unwanted_label:
                        unwanted_conf_ids.add(unwanted_conf_id)
        
        # Dicts
        pkgs_unwanted_buildroot = {}
        pkgs_unwanted_completely = {}
        srpms_unwanted_buildroot = {}
        srpms_unwanted_completely = {}

        # Populate the dicts
        for unwanted_conf_id in unwanted_conf_ids:
            unwanted_conf = self.configs["unwanteds"][unwanted_conf_id]

            # Pkgs
            for pkg_name in unwanted_conf["unwanted_packages"]:
                if pkg_name not in pkgs_unwanted_completely:
                    pkgs_unwanted_completely[pkg_name] = set()
                pkgs_unwanted_completely[pkg_name].add(unwanted_conf_id)

            # Arch Pkgs
            for pkg_name in unwanted_conf["unwanted_arch_packages"][arch]:
                if pkg_name not in pkgs_unwanted_completely:
                    pkgs_unwanted_completely[pkg_name] = set()
                pkgs_unwanted_completely[pkg_name].add(unwanted_conf_id)

            # SRPMs
            for pkg_source_name in unwanted_conf["unwanted_source_packages"]:
                if pkg_source_name not in srpms_unwanted_completely:
                    srpms_unwanted_completely[pkg_source_name] = set()
                srpms_unwanted_completely[pkg_source_name].add(unwanted_conf_id)

        # Add it to the packages
        for pkg_id, pkg in view["pkgs"].items():
            pkg_name = pkg["name"]
            srpm_name = pkg["source_name"]

            if pkg_name in pkgs_unwanted_completely:
                list_ids = pkgs_unwanted_completely[pkg_name]
                view["pkgs"][pkg_id]["unwanted_completely_in_list_ids"].update(list_ids)

            if srpm_name in srpms_unwanted_completely:
                list_ids = srpms_unwanted_completely[srpm_name]
                view["pkgs"][pkg_id]["unwanted_completely_in_list_ids"].update(list_ids)
        
        # Add it to the srpms
        for srpm_id, srpm in view["source_pkgs"].items():
            srpm_name = srpm["name"]

            if srpm_name in srpms_unwanted_completely:
                list_ids = srpms_unwanted_completely[srpm_name]
                view["source_pkgs"][srpm_id]["unwanted_completely_in_list_ids"].update(list_ids)


    def _add_unwanted_packages_to_views(self):

        log("")
        log("Adding Unwanted Packages to views...")
        log("")

        # First, the standard views
        for view_conf_id in self.configs["views"]:
            view_conf = self.configs["views"][view_conf_id]

            if view_conf["type"] == "compose":
                if view_conf["buildroot_strategy"] == "root_logs":
                    for arch in view_conf["architectures"]:

                        view_id = "{view_conf_id}:{arch}".format(
                            view_conf_id=view_conf_id,
                            arch=arch
                        )

                        view = self.data["views"][view_id]

                        self._add_unwanted_packages_to_view(view, view_conf)


    def _recommend_maintainers(self):

        # Packages can be on one or more _levels_:
        #   level 0 is runtime
        #   level 1 is build deps of the previous level
        #   level 2 is build deps of the previous level
        #   ... etc.
        #
        # Within a level, they can be on one or more _sublevels_:
        #   level 0 sublevel 0 is explicitly required
        #   level 0 sublevel 1 is runtiem deps of the previous sublevel
        #   level 0 sublevel 2 is runtiem deps of the previous sublevel
        #   ... etc
        #   level 1 sublevel 0 is direct build deps of the previous level
        #   level 1 sublevel 1 is runtime deps of the previous sublevel
        #   level 1 sublevel 2 is runtiem deps of the previous sublevel
        #   ... etc
        #
        # I'll call a combination of these a _score_ because I can't think of
        # anything better at this point. It's a tuple! 
        # 
        # (0, 0)
        #  |  '-- sub-level 0 == explicitly required
        #  '---- level 0 == runtime
        # 


        for view_conf_id in self.configs["views"]:
            view_conf = self.configs["views"][view_conf_id]
            view_all_arches = self.data["views_all_arches"][view_conf_id]

            # Skip addons for now
            # TODO: Implement support for addons
            if view_conf["type"] == "addon":
                continue

            log("  {}".format(view_conf_id))

            # Level 0
            level = str(0)
            sublevel = str(0)
            score = (level, sublevel)

            log("    {}".format(score))

            # There's not much point in analyzing packages on multple levels.
            # For example, if someone explicitly requires glibc, I don't need to track
            # details up until the very end of the dependency chain...
            this_level_srpms = set()
            previous_level_srpms = set()

            pkgs_by_name = view_all_arches["pkgs_by_name"]
            source_pkgs_by_name = view_all_arches["source_pkgs_by_name"]
            workloads_data = self.data["workloads"]
            workloads_configs = self.configs["workloads"]

            for pkg_name, pkg in pkgs_by_name.items():
                pkg_rec = pkg["maintainer_recommendation"]
                pkg_rec_details = pkg["maintainer_recommendation_details"]

                for workload_id in pkg["in_workload_ids_req"]:
                    workload = workloads_data[workload_id]
                    workload_conf_id = workload["workload_conf_id"]
                    workload_maintainer = workloads_configs[workload_conf_id]["maintainer"]

                    if workload_maintainer not in pkg_rec:
                        pkg_rec[workload_maintainer] = set()
                    pkg_rec[workload_maintainer].add(score)

                    level_details = pkg_rec_details.setdefault(level, {})
                    sublevel_details = level_details.setdefault(sublevel, {})
                    if workload_maintainer not in sublevel_details:
                        sublevel_details[workload_maintainer] = {"reasons": set(), "locations": set()}
                    sublevel_details[workload_maintainer]["locations"].add(workload_conf_id)

            level_changes_made = True
            level_change_detection = set()

            while level_changes_made:

                if int(level) > 0:
                    level_changes_made = False
                    log("    {}".format(score))

                    for pkg_name, pkg in pkgs_by_name.items():
                        if pkg["source_name"] in previous_level_srpms:
                            continue

                        pkg_rec = pkg["maintainer_recommendation"]
                        pkg_rec_details = pkg["maintainer_recommendation_details"]

                        for buildroot_srpm_name in pkg["in_buildroot_of_srpm_name_req"]:
                            buildroot_srpm = source_pkgs_by_name[buildroot_srpm_name]
                            br_rec = buildroot_srpm["maintainer_recommendation"]

                            prev_sublevels = set()
                            for br_maint, br_scores in br_rec.items():
                                for br_score in br_scores:
                                    if br_score[0] == prev_level:
                                        prev_sublevels.add(br_score[1])
                            if not prev_sublevels:
                                continue
                            target_score = (prev_level, min(prev_sublevels))

                            for br_maint, br_scores in br_rec.items():
                                if target_score not in br_scores:
                                    continue

                                detect_tuple = (buildroot_srpm_name, pkg_name)
                                if detect_tuple not in level_change_detection:
                                    level_changes_made = True
                                    level_change_detection.add(detect_tuple)

                                if br_maint not in pkg_rec:
                                    pkg_rec[br_maint] = set()
                                pkg_rec[br_maint].add(score)

                                level_details = pkg_rec_details.setdefault(level, {})
                                sublevel_details = level_details.setdefault(sublevel, {})
                                if br_maint not in sublevel_details:
                                    sublevel_details[br_maint] = {"reasons": set(), "locations": set()}
                                sublevel_details[br_maint]["locations"].add(buildroot_srpm_name)

                sublevel_changes_made = True
                sublevel_change_detection = set()

                while sublevel_changes_made:
                    sublevel_changes_made = False
                    prev_score = score
                    prev_sublevel = sublevel
                    sublevel = str(int(sublevel) + 1)
                    score = (level, sublevel)
                    log("    {}".format(score))

                    for pkg_name, pkg in pkgs_by_name.items():
                        if pkg["source_name"] in previous_level_srpms:
                            continue

                        pkg_rec = pkg["maintainer_recommendation"]
                        pkg_rec_details = pkg["maintainer_recommendation_details"]

                        for superior_pkg_name in pkg["hard_dependency_of_pkg_names"]:
                            superior_pkg = pkgs_by_name[superior_pkg_name]
                            superior_srpm_name = superior_pkg["source_name"]

                            for sup_maint, sup_scores in superior_pkg["maintainer_recommendation"].items():
                                if prev_score not in sup_scores:
                                    continue

                                detect_tuple = (superior_pkg_name, pkg_name, sup_maint)
                                if detect_tuple in sublevel_change_detection:
                                    continue
                                sublevel_changes_made = True
                                sublevel_change_detection.add(detect_tuple)

                                if sup_maint not in pkg_rec:
                                    pkg_rec[sup_maint] = set()
                                pkg_rec[sup_maint].add(score)

                                level_details = pkg_rec_details.setdefault(level, {})
                                sublevel_details = level_details.setdefault(sublevel, {})
                                if sup_maint not in sublevel_details:
                                    sublevel_details[sup_maint] = {"reasons": set(), "locations": set()}

                                sup_locations = superior_pkg["maintainer_recommendation_details"][level][prev_sublevel][sup_maint]["locations"]
                                sublevel_details[sup_maint]["locations"].update(sup_locations)
                                sublevel_details[sup_maint]["reasons"].add((superior_pkg_name, superior_srpm_name, pkg_name))

                for pkg_name, pkg in pkgs_by_name.items():
                    source_name = pkg["source_name"]
                    srpm_entry = source_pkgs_by_name[source_name]

                    for maintainer, maintainer_scores in pkg["maintainer_recommendation"].items():
                        srpm_rec = srpm_entry["maintainer_recommendation"]
                        if maintainer not in srpm_rec:
                            srpm_rec[maintainer] = set()
                        srpm_rec[maintainer].update(maintainer_scores)
                        this_level_srpms.add(source_name)

                    srpm_rec_details = srpm_entry["maintainer_recommendation_details"]
                    for loop_level, loop_sublevels in pkg["maintainer_recommendation_details"].items():
                        level_d = srpm_rec_details.setdefault(loop_level, {})
                        for loop_sublevel, maintainers in loop_sublevels.items():
                            sublevel_d = level_d.setdefault(loop_sublevel, {})
                            for maintainer, maint_details in maintainers.items():
                                if maintainer not in sublevel_d:
                                    sublevel_d[maintainer] = {"reasons": set(), "locations": set()}
                                sublevel_d[maintainer]["reasons"].update(maint_details["reasons"])
                                sublevel_d[maintainer]["locations"].update(maint_details["locations"])

                prev_level = level
                level = str(int(level) + 1)
                sublevel = str(0)
                score = (level, sublevel)
                previous_level_srpms.update(this_level_srpms)
                this_level_srpms = set()

            for source_name, srpm in source_pkgs_by_name.items():
                rec_details = srpm["maintainer_recommendation_details"]
                if not rec_details:
                    continue

                lowest_level_int = min(int(k) for k in rec_details)
                lowest_level = str(lowest_level_int)

                if not rec_details[lowest_level]:
                    continue

                lowest_sublevel = str(min(int(k) for k in rec_details[lowest_level]))
                best_level_data = rec_details[lowest_level][lowest_sublevel]

                highest_deps = 0
                best_maintainers = set()
                use_locations = lowest_level_int > 0 and lowest_sublevel == "0"

                for maint, maint_data in best_level_data.items():
                    ndeps = len(maint_data["locations"]) if use_locations else len(maint_data["reasons"])
                    if ndeps > highest_deps:
                        highest_deps = ndeps
                        best_maintainers = set()
                    if ndeps == highest_deps:
                        best_maintainers.add(maint)

                srpm["best_maintainers"].update(best_maintainers)

        log("")
        log("  DONE!")
        log("")


    def analyze_things(self):
        log("")
        log("###############################################################################")
        log("### Analyzing stuff! ##########################################################")
        log("###############################################################################")
        log("")

        self._record_metric("started analyze_things()")

        self.data["pkgs"] = {}
        self.data["envs"] = {}
        self.data["workloads"] = {}
        self.data["views"] = {}

        with tempfile.TemporaryDirectory() as tmp:

            if self.settings["dnf_cache_dir_override"]:
                self.tmp_dnf_cachedir = self.settings["dnf_cache_dir_override"]
            else:
                self.tmp_dnf_cachedir = os.path.join(tmp, "dnf_cachedir")
            self.tmp_installroots = os.path.join(tmp, "installroots")

            # List of supported arches
            all_arches = self.settings["allowed_arches"]

            # Repos
            log("")
            log("=====  Analyzing Repos =====")
            log("")
            self._analyze_repos()

            self._record_metric("finished _analyze_repos()")

            # Compute repo fingerprints for incremental cache
            log("")
            log("=====  Computing repo fingerprints =====")
            log("")
            self._compute_repo_fingerprints()

            # Pre-check which workloads are cached so we can skip unchanged envs
            log("")
            log("=====  Pre-checking incremental cache =====")
            log("")
            env_all_cached = self._pre_check_workload_cache()

            # Environments
            log("")
            log("=====  Analyzing Environments =====")
            log("")
            self._analyze_envs(env_all_cached=env_all_cached)

            self._record_metric("finished _analyze_envs()")

            # Workloads
            log("")
            log("=====  Analyzing Workloads =====")
            log("")
            self._analyze_workloads()

            self._record_metric("finished _analyze_workloads()")

            # Views
            #
            # This creates:
            #    data["views"][view_id]["id"]
            #    data["views"][view_id]["view_conf_id"]
            #    data["views"][view_id]["arch"]
            #    data["views"][view_id]["workload_ids"]
            #    data["views"][view_id]["pkgs"]
            #    data["views"][view_id]["source_pkgs"]
            #
            log("")
            log("=====  Analyzing Views =====")
            log("")
            self._analyze_views()

            self._record_metric("finished _analyze_views()")

            # Buildroot
            # This is partially similar to workloads, because it's resolving
            # the full dependency tree of the direct build dependencies of SRPMs
            #
            # So compared to workloads:
            #   direct build dependencies are like required packages in workloads
            #   the dependencies are like dependencies in workloads
            #   the "build" group is like environments in workloads
            #
            # This completely creates:
            #   data["buildroot"]["koji_srpms"][koji_id][arch][srpm_id]...
            #   data["buildroot"]["srpms"][repo_id][arch][srpm_id]...
            # 
            log("")
            log("=====  Analyzing Buildroot =====")
            log("")
            self._analyze_buildroot()

            self._record_metric("finished _analyze_buildroot()")

            # Add buildroot packages to views
            # 
            # Further extends the following with buildroot packages:
            #   data["views"][view_id]["pkgs"]
            #   data["views"][view_id]["source_pkgs"]
            #
            log("")
            log("=====  Adding Buildroot to Views =====")
            log("")
            self._add_buildroot_to_views()

            self._record_metric("finished _add_buildroot_to_views()")

            # Unwanted packages
            log("")
            log("=====  Adding Unwanted Packages to Views =====")
            log("")
            self._add_unwanted_packages_to_views()

            self._record_metric("finished _add_unwanted_packages_to_views()")

            # Generate combined views for all arches
            log("")
            log("=====  Generating views_all_arches =====")
            log("")
            self._generate_views_all_arches()

            self._record_metric("finished _generate_views_all_arches()")

            # Recommend package maintainers in views
            log("")
            log("=====  Recommending maintainers =====")
            log("")
            self._recommend_maintainers()

            self._record_metric("finished _recommend_maintainers()")


            # Finally, save the caches for next time
            try:
                dump_data(self.settings["root_log_deps_cache_path"], self.cache["root_log_deps"]["next"])
            except PermissionError:
                log("Warning: Could not write root log deps cache (permission denied)")

            self._record_metric("finished dumping the root log data cache")

            # Save the incremental cache
            log("")
            log("=====  Saving incremental cache =====")
            log("")
            log(f"  Cache stats: {self._cache_hits} hits, {self._cache_misses} misses")
            self._save_incremental_cache()

            self._record_metric("finished saving incremental cache")


        self._record_metric("finished analyze_things()")           

        return self.data
