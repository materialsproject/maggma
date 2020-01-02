import re

from invoke import task
from monty.os import cd

from maggma import __version__


@task
def setver(c, patch=False, new_ver=""):
    if (not patch and not new_ver) or (patch and new_ver):
        raise Exception("Either use --patch or specify " 'e.g. --new-ver="x.y.z".')
    if patch:
        v = [int(x) for x in __version__.split(".")]
        v[2] += 1
        new_ver = ".".join(map(str, v))
    with open("maggma/__init__.py", "r") as f:
        lines = [
            re.sub("__version__ = .+", '__version__ = "{}"'.format(new_ver), l.rstrip())
            for l in f
        ]
    with open("maggma/__init__.py", "w") as f:
        f.write("\n".join(lines))

    with open("setup.py", "r") as f:
        lines = [
            re.sub("version=([^,]+),", 'version="{}",'.format(new_ver), l.rstrip())
            for l in f
        ]
    with open("setup.py", "w") as f:
        f.write("\n".join(lines))
        f.write("\n")
    print("Bumped version to {}".format(new_ver))


@task
def publish(c):
    c.run("rm dist/*.*", warn=True)
    c.run("python setup.py sdist bdist_wheel")
    c.run("twine upload dist/*")


@task
def makedoc(c, preview=False, port=8000):
    c.run("sphinx-apidoc --separate -d 6 -o build -f maggma")
    c.run("make html")
    if preview:
        with cd("build/html"):
            print("Serving docs preview at http://localhost:{}".format(port))
            c.run("python -m http.server {}".format(port))


@task
def publish_doc(c):
    c.run("git checkout master")
    c.run("git stash")
    c.run("git checkout gh-pages")
    c.run("cp -r build/html/* .")
    c.run("rm -r _sources")
    c.run("git add .")
    c.run("git commit -am 'Publish docs'")
    c.run("git push origin gh-pages")
    c.run("git checkout master")
    c.run("git stash pop")
