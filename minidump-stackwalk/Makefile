BREAKPAD_SRCDIR := ../stackwalk/src
BREAKPAD_OBJDIR := ../stackwalk
BREAKPAD_LIBS := \
  $(BREAKPAD_OBJDIR)/lib/libbreakpad.a \
  $(BREAKPAD_OBJDIR)/lib/libdisasm.a \
  $(NULL)

JSON_DIR := jsoncpp-src-0.5.0
JSON_SRCDIR := $(JSON_DIR)/src/lib_json
JSON_INCLUDEDIR := $(JSON_DIR)/include

BINS := stackwalker dumplookup

all: $(BINS)

stackwalker_OBJS := \
  json_reader.o \
  json_value.o \
  json_writer.o \
  $(NULL)

VPATH += $(JSON_SRCDIR)
CXXFLAGS += -I$(JSON_INCLUDEDIR) -std=gnu++0x -Wno-format -Werror

.SECONDEXPANSION:
$(BINS): %: %.cc $(BREAKPAD_LIBS) $$($$*_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $< $(BREAKPAD_LIBS) $($*_OBJS) -I$(BREAKPAD_SRCDIR)


clean:
	$(RM) $(BINS) *.o
