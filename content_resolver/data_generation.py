import os
from content_resolver.utils import dump_data, log


def _generate_json_file(data, page_name, settings):
    output = settings["output"]
    filename = f"{page_name.replace(':', '--')}.json"
    dump_data(os.path.join(output, filename), data)


def _generate_txt_file(data_list, file_name, settings):
    file_contents = "\n".join(data_list)
    filename = f"{file_name.replace(':', '--')}.txt"
    output = settings["output"]
    with open(os.path.join(output, filename), "w") as file:
        file.write(file_contents)


def _generate_view_lists(query):
    log("Generating view lists...")

    for view_conf_id, view_conf in query.configs["views"].items():

        all_arches_lists = {}

        for arch in view_conf["architectures"]:

            lists = {k: set() for k in [
                "view-all-binary-package-list", "view-all-binary-package-nevr-list",
                "view-all-binary-package-name-list", "view-all-source-package-list",
                "view-all-source-package-name-list", "view-binary-package-list",
                "view-binary-package-nevr-list", "view-binary-package-name-list",
                "view-source-package-list", "view-source-package-name-list",
                "view-buildroot-package-list", "view-buildroot-package-nevr-list",
                "view-buildroot-package-name-list", "view-buildroot-source-package-list",
                "view-buildroot-source-package-name-list",
            ]}

            view_id = f"{view_conf_id}:{arch}"
            view = query.data["views"][view_id]

            for pkg_id, pkg in view["pkgs"].items():
                lists["view-all-binary-package-list"].add(pkg_id)
                lists["view-all-binary-package-nevr-list"].add(pkg["nevr"])
                lists["view-all-binary-package-name-list"].add(pkg["name"])

                srpm_id = pkg["sourcerpm"].rsplit(".src.rpm")[0]
                lists["view-all-source-package-list"].add(srpm_id)
                lists["view-all-source-package-name-list"].add(pkg["source_name"])

                if pkg["in_workload_ids_all"]:
                    lists["view-binary-package-list"].add(pkg_id)
                    lists["view-binary-package-nevr-list"].add(pkg["nevr"])
                    lists["view-binary-package-name-list"].add(pkg["name"])
                    lists["view-source-package-list"].add(srpm_id)
                    lists["view-source-package-name-list"].add(pkg["source_name"])
                else:
                    lists["view-buildroot-package-list"].add(pkg_id)
                    lists["view-buildroot-package-nevr-list"].add(pkg["nevr"])
                    lists["view-buildroot-package-name-list"].add(pkg["name"])
                    lists["view-buildroot-source-package-list"].add(srpm_id)
                    lists["view-buildroot-source-package-name-list"].add(pkg["source_name"])

            for list_name, list_content in lists.items():
                file_name = f"{list_name}--{view_conf_id}--{arch}"
                _generate_txt_file(sorted(list_content), file_name, query.settings)

                if list_name not in all_arches_lists:
                    all_arches_lists[list_name] = set()
                all_arches_lists[list_name].update(list_content)

        for list_name, list_content in all_arches_lists.items():
            file_name = f"{list_name}--{view_conf_id}"
            _generate_txt_file(sorted(list_content), file_name, query.settings)

    log("Done!")
    log("")


def _generate_env_json_files(query):

    log("Generating JSON files for environments...")
    
    # == envs
    log("")
    log("Envs:")
    for env_conf_id, env_conf in query.configs["envs"].items():

        # === Config

        log("")
        log("  Config for: {}".format(env_conf_id))

        # Where to save
        data_name = "env-conf--{env_conf_id_slug}".format(
            env_conf_id_slug = query.url_slug_id(env_conf_id)
        )

        # What to save
        output_data = {}
        output_data["id"] = env_conf_id
        output_data["type"] = "env_conf"
        output_data["data"] = query.configs["envs"][env_conf_id]

        # And save it
        _generate_json_file(output_data, data_name, query.settings)


        # === Results

        for env_id in query.envs(env_conf_id, None, None, list_all=True):
            env = query.data["envs"][env_id]

            log("  Results: {}".format(env_id))

            # Where to save
            data_name = "env--{env_id_slug}".format(
                env_id_slug = query.url_slug_id(env_id)
            )

            # What to save
            output_data = {}
            output_data["id"] = env_id
            output_data["type"] = "env"
            output_data["data"] = query.data["envs"][env_id]
            output_data["pkg_query"] = query.env_pkgs_id(env_id)

            # And save it
            _generate_json_file(output_data, data_name, query.settings)

    log("  Done!")
    log("")


def _generate_workload_json_files(query):

    log("Generating JSON files for workloads...")

    # == Workloads
    log("")
    log("Workloads:")
    for workload_conf_id, workload_conf in query.configs["workloads"].items():

        # === Config

        log("")
        log("  Config for: {}".format(workload_conf_id))

        # Where to save
        data_name = "workload-conf--{workload_conf_id_slug}".format(
            workload_conf_id_slug = query.url_slug_id(workload_conf_id)
        )

        # What to save
        output_data = {}
        output_data["id"] = workload_conf_id
        output_data["type"] = "workload_conf"
        output_data["data"] = query.configs["workloads"][workload_conf_id]

        # And save it
        _generate_json_file(output_data, data_name, query.settings)


        # === Results

        for workload_id in query.workloads(workload_conf_id, None, None, None, list_all=True):
            workload = query.data["workloads"][workload_id]

            log("  Results: {}".format(workload_id))

            # Where to save
            data_name = "workload--{workload_id_slug}".format(
                workload_id_slug = query.url_slug_id(workload_id)
            )

            # What to save
            output_data = {}
            output_data["id"] = workload_id
            output_data["type"] = "workload"
            output_data["data"] = query.data["workloads"][workload_id]
            output_data["pkg_query"] = query.workload_pkgs_id(workload_id)

            # And save it
            _generate_json_file(output_data, data_name, query.settings)

    log("  Done!")
    log("")


def _generate_view_json_files(query):

    log("Generating JSON files for views...")
    for view_conf_id, view_conf in query.configs["views"].items():
        view_all_arches = query.data["views_all_arches"][view_conf_id]

        # =================================================================
        # view-packages
        # =============

        # Where to save
        data_name = "view-packages--{view_id_slug}".format(
            view_id_slug = query.url_slug_id(view_conf_id)
        )

        log("  {}".format(data_name))

        # What to save
        output_data = {}
        output_data["id"] = view_conf_id
        output_data["pkgs"] = {}

        keys_to_save = [
            "name",
            "source_name",
            "arches_arches",
            "placeholder",
            "hard_dependency_of_pkg_nevrs",
            "weak_dependency_of_pkg_nevrs",
            "in_workload_conf_ids_req",
            "level_number"
        ]

        for pkg_id, pkg in view_all_arches["pkgs_by_nevr"].items():
            output_data["pkgs"][pkg_id] = {}

            for key in keys_to_save:
                output_data["pkgs"][pkg_id][key] = pkg[key]

        # And save it
        _generate_json_file(output_data, data_name, query.settings)

        # =================================================================
        # view-srpms (components, including ownership recommendations)
        # =============

        # Where to save
        data_name = "view-sources--{view_id_slug}".format(
            view_id_slug = query.url_slug_id(view_conf_id)
        )

        log("  {}".format(data_name))

        # What to save
        output_data = {}
        output_data["id"] = view_conf_id
        output_data["srpms"] = {}

        keys_to_save = [
            "name",
            "arches",
            "best_maintainers",
            "level_number",
            "in_workload_conf_ids_env",
            "in_workload_conf_ids_req",
            "in_workload_conf_ids_dep",
            "in_buildroot_of_srpm_name_req",
            "in_buildroot_of_srpm_name_dep",
            "level_number",
        ]

        for srpm_name, srpm in view_all_arches["source_pkgs_by_name"].items():
            output_data["srpms"][srpm_name] = {}

            for key in keys_to_save:
                output_data["srpms"][srpm_name][key] = srpm[key]

        # And save it
        _generate_json_file(output_data, data_name, query.settings)

        # =================================================================
        # view-workloads
        # =============

        # Where to save
        data_name = "view-workloads--{view_id_slug}".format(
            view_id_slug = query.url_slug_id(view_conf_id)
        )

        log("  {}".format(data_name))

        # What to save
        output_data = {}
        output_data["id"] = view_conf_id
        output_data["workloads"] = view_all_arches["workloads"]


        # And save it
        _generate_json_file(output_data, data_name, query.settings)


    log("  Done!")
    log("")


def _generate_maintainers_json_file(query):

    log("Generating the maintainers json file...")

    maintainer_data = query.maintainers()
    _generate_json_file(maintainer_data, "maintainers", query.settings)

    log("  Done!")
    log("")


def generate_data_files(query):

    log("")
    log("###############################################################################")
    log("### Generating data files! ####################################################")
    log("###############################################################################")
    log("")

    # Generate the package lists for views
    _generate_view_lists(query)

    # Generate the JSON files for envs 
    _generate_env_json_files(query)

    # Generate the JSON files for workloads 
    _generate_workload_json_files(query)

    # Generate the JSON files for views
    _generate_view_json_files(query)

    # Generate data for the top-level results pages
    _generate_maintainers_json_file(query)



