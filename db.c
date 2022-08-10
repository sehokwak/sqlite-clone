#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>


typedef struct InputBuffer {
  char *buffer;
  size_t buffer_length; 
  ssize_t input_length; 
} InputBuffer_t;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_SYNTAX_ERROR,
  PREPARE_STRING_TOO_LONG,
  PREPARE_NEGATIVE_ID
} PrepareResult;

typedef enum {
  STATEMENT_INSERT,
  STATEMENT_SELECT
} StatementType;

typedef enum {
  EXECUTE_SUCCESS,
  EXECUTE_TABLE_FULL
} ExecuteResult;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct Row {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE + 1];
  char email[COLUMN_EMAIL_SIZE + 1];
} Row_t;

typedef struct Statement {
  StatementType type;
  Row_t row_to_insert;
} Statement_t;


// Compact representation of a row in table
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
const uint32_t ID_SIZE = size_of_attribute(Row_t, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row_t, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row_t, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

// Convert from compact to row
void serialize_row(Row_t *source, void *destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

// Convert from row to compact
void deserialize_row(void *source, Row_t *destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}


const uint32_t PAGE_SIZE = 4096;
#define TABLE_MAX_PAGES 100
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef struct Pager {
  int file_descriptor;
  uint32_t file_length;
  void *pages[TABLE_MAX_PAGES];
} Pager_t;

typedef struct Table {
  uint32_t num_rows;
  Pager_t *pager;
} Table_t;



InputBuffer_t *new_input_buffer() { // Constructor function 
  InputBuffer_t *input_buffer = (InputBuffer_t *)malloc(sizeof(InputBuffer_t));
  input_buffer->buffer = NULL; 
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

void print_prompt() { printf("db > "); }

void print_row(Row_t *row) {
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void read_input(InputBuffer_t *input_buffer) {
  ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) { // getline return -1 on failure
    printf("Error reading input/n");
    exit(EXIT_FAILURE);
  }

  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer_t *input_buffer) {
  free(input_buffer->buffer);
  free(input_buffer);
}



PrepareResult prepare_insert(InputBuffer_t *input_buffer, Statement_t *statement) {
  statement->type = STATEMENT_INSERT;

  char *keyword   = strtok(input_buffer->buffer, " ");
  char *id_string = strtok(NULL, " ");
  char *username  = strtok(NULL, " ");
  char *email     = strtok(NULL, " ");

  if (id_string == NULL || username == NULL || email == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }
  if (strlen(username) > COLUMN_USERNAME_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }
  if (strlen(email) > COLUMN_EMAIL_SIZE) {
    return PREPARE_STRING_TOO_LONG;
  }
  
  int id = atoi(id_string);
  if (id < 0) {
    return PREPARE_NEGATIVE_ID;
  }
  statement->row_to_insert.id = id;
  strcpy(statement->row_to_insert.username, username);
  strcpy(statement->row_to_insert.email, email);

  return PREPARE_SUCCESS;
}

// SQL Command Processor
PrepareResult prepare_statement(InputBuffer_t *input_buffer, Statement_t *statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  } 
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}


// Gets page number from page cache
// Handles cache misses
void *get_page(Pager_t *pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d > %d \n", page_num, TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }

  // If cache miss then allocate memory and load from file
  if (pager->pages[page_num] == NULL) {
    void *page = malloc(PAGE_SIZE);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;

    // Handles last partial page
    if (pager->file_length % PAGE_SIZE) {
      num_pages += 1;
    }

    if (page_num <= num_pages) {
      // lseek repositions the open fd at specified offset 
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET); 
      
      // reads up to PAGE_SIZE bytes from fd into page
      // returns num of bytes read
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read == -1) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }
    pager->pages[page_num] = page;
  }

  // Else return from page cache
  return pager->pages[page_num];
}

// Finds where to read/write in memory
void *row_slot(Table_t *table, uint32_t row_num) {
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void *page = get_page(table->pager, page_num);
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return page + byte_offset;
}

// Executes INSERT into table
ExecuteResult execute_insert(Statement_t *statement, Table_t* table) {
  if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }

  Row_t *row_to_insert = &(statement->row_to_insert);

  serialize_row(row_to_insert, row_slot(table, table->num_rows));
  table->num_rows += 1;

  return EXECUTE_SUCCESS;
}

// Executes SELECT command, printing table 
ExecuteResult execute_select(Statement_t *statement, Table_t* table) {
  Row_t row;
  for (uint32_t i = 0; i < table->num_rows; i++) {
    deserialize_row(row_slot(table, i), &row);
    print_row(&row);
  }
  return EXECUTE_SUCCESS;
}

// Tokenizes SQL command
ExecuteResult execute_statement(Statement_t *statement, Table_t* table) {
  switch (statement->type) {
    case (STATEMENT_INSERT):
      return execute_insert(statement, table);
    case (STATEMENT_SELECT):
      return execute_select(statement, table);
  }
}







// Initializes Page cache to all NULLs
Pager_t *pager_open(const char* filename) {
  int fd = open(filename, 
    O_RDWR |  // Read/Write mode
    O_CREAT,  // Create file if it doesn't exist
    S_IWUSR | // User write permission
    S_IRUSR   // User read permission
  );

  if (fd == -1) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager_t *pager = malloc(sizeof(Pager_t));
  pager->file_descriptor = fd;
  pager->file_length = file_length;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

// Opens connection to database
// Initializes Table and Pager
Table_t * open_db(const char *filename) {
  Pager_t *pager = pager_open(filename);
  uint32_t num_rows = pager->file_length / ROW_SIZE;

  Table_t *table = malloc(sizeof(Table_t));
  table->pager = pager;
  table->num_rows = num_rows;

  return table;
}


// 
void pager_flush(Pager_t *pager, uint32_t page_num, uint32_t size) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page.\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

  if (offset == -1) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);

  if (bytes_written == -1) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}


// Close database connection, flush page cache, free malloc
void close_db(Table_t *table) {
  Pager_t *pager = table->pager;
  uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

  // Flush pages that are full
  for (uint32_t i = 0; i < num_full_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i, PAGE_SIZE);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  // Handle flushing last partial page
  uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
  if (num_additional_rows > 0) {
    uint32_t page_num = num_full_pages;
    if (pager->pages[page_num] != NULL) {
      pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
      free(pager->pages[page_num]);
      pager->pages[page_num] = NULL;
    }
  }

  // Free allocated memory
  int result = close(pager->file_descriptor);
  if (result < 0) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }
  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void *page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }
  free(pager);
  free(table);
}


// Executes meta commands ie commands that start with '.'
MetaCommandResult do_meta_command(InputBuffer_t *input_buffer, Table_t *table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    close_input_buffer(input_buffer);
    close_db(table);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}


int main(int argc, char *argv[]) {
  if (argc < 2) {
    printf("Please provide path for database.\n");
    exit(EXIT_FAILURE);
  }

  char *filename = argv[1];
  Table_t *table = open_db(filename);
  InputBuffer_t *input_buffer = new_input_buffer();

  while(true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      switch (do_meta_command(input_buffer, table)) {
        case (META_COMMAND_SUCCESS):
          continue;
        case (META_COMMAND_UNRECOGNIZED_COMMAND):
          printf("Unrecognized command '%s'\n", input_buffer->buffer);
          continue;
      }
    }

    Statement_t statement;
    switch (prepare_statement(input_buffer, &statement)) {
      case (PREPARE_SUCCESS):
        break;
      case(PREPARE_STRING_TOO_LONG):
        printf("String is too long.\n");
        continue;
      case(PREPARE_NEGATIVE_ID):
        printf("ID cannot be negative.\n");
        continue;
      case (PREPARE_SYNTAX_ERROR):
        printf("Syntax error. Less than 3 args provided.\n");
        continue;
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buffer);
        continue;
    }

    switch (execute_statement(&statement, table)) {
      case (EXECUTE_SUCCESS):
        printf("Executed.\n");
        break;
      case (EXECUTE_TABLE_FULL):
        printf("Error: Table max size exceeded.\n");
        break;
    }
  }
}