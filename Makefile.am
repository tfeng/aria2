SUBDIRS =  po lib deps src doc test

ACLOCAL_AMFLAGS = -I m4 --install
RST2HTML = @RST2HTML@

EXTRA_DIST = config.rpath \
	     script-helper \
	     makerelease \
	     mingw-config mingw-release mingw-build-memo \
	     android-config \
	     makerelease-osx.mk osx-package/DS_Store osx-package/etc/paths.d/aria2c osx-package/etc/manpaths.d/aria2 \
	     Dockerfile.mingw \
	     Dockerfile.raspberrypi \
	     examples/libaria2ex.cc examples/libaria2wx.cc

dist_doc_DATA = README README.rst README.html

.PHONY: clang-format

if HAVE_RST2HTML
README.html: README.rst
	$(RST2HTML)  $< > $@
else # !HAVE_RST2HTML
README.html: README.rst
	@echo Warning: rst2html not installed. Placing only a dummy file for $@
	echo "<!doctype html>" > $@
	echo "<p>Readme has not been built!.<br>Instead see <a href='README.rst'>README.rst</a></p>" >> $@
endif # !HAVE_RST2HTML

dist_noinst_DATA = LICENSE.OpenSSL

# Format source files using clang-format.  Don't format source files
# under deps directory since we are not responsible for their coding
# style.
clang-format:
	CLANGFORMAT=`git config --get clangformat.binary`; \
	test -z $${CLANGFORMAT} && CLANGFORMAT="clang-format"; \
	$${CLANGFORMAT} -i $(top_srcdir)/src/*.{c,cc,h} $(top_srcdir)/src/includes/aria2/*.h \
	$(top_srcdir)/examples/*.cc $(top_srcdir)/test/*.{cc,h}

# FXPlayer: `make install` also installs the Telegram helper (aria2c-tg, see src/FxTelegram.h) next to aria2c, as
# configure's --with-tdlib[=DIR] decides (FX_TDLIB_HELPER; empty = --without-tdlib). The helper is built by
# fetch-tdlib-deps.sh (CMake, against its pinned TDLib), not by configure/make. Without --with-tdlib a missing helper
# is skipped with a note; with it, configure already made sure it exists.
install-exec-local:
	@if test -z "$(FX_TDLIB_HELPER)"; then \
	  :; \
	elif test -x "$(FX_TDLIB_HELPER)"; then \
	  echo " $(INSTALL_PROGRAM) $(FX_TDLIB_HELPER) '$(DESTDIR)$(bindir)'"; \
	  $(MKDIR_P) "$(DESTDIR)$(bindir)" && \
	  $(INSTALL_PROGRAM) "$(FX_TDLIB_HELPER)" "$(DESTDIR)$(bindir)/aria2c-tg"; \
	elif test "$(fx_tdlib_required)" = yes; then \
	  echo "error: $(FX_TDLIB_HELPER) is missing (./fetch-tdlib-deps.sh)" >&2; exit 1; \
	else \
	  echo "aria2c-tg not built (./fetch-tdlib-deps.sh): Telegram downloads stay unavailable"; \
	fi

uninstall-local:
	@if test -n "$(FX_TDLIB_HELPER)"; then rm -f "$(DESTDIR)$(bindir)/aria2c-tg"; fi
