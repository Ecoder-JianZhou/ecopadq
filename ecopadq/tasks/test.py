# import importlib
# ls_spec_sites  = ["SPRUCE"]
# ls_spec_models = ["TECO_SPRUCE","matrix_models","all"]  # Jian: "all" is used to test the TECO_SPRUCE AND matrix_models at SPRUCE site
# dict_sites     = {}
# for iSite in ls_spec_sites:
#     script_site = iSite + "_tasks"  
#     dict_sites[iSite] = importlib.import_module(script_site)

# import importlib.util

# site ="SPRUCE_tasks"
# spec = importlib.util.spec_from_file_location(site, "./"+site+".py")

# foo = importlib.util.module_from_spec(spec)

# spec.loader.exec_module(foo)

# print(foo.test())

aaa = "SPRUCE_tasks"
exec("import "+ aaa +" as a")
a.test()