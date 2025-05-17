"""Tests for conditional execution feature in SQLFlow."""

from sqlflow.parser.ast import (
    ConditionalBlockStep,
    ConditionalBranchStep,
    Pipeline,
    SQLBlockStep,
)
from sqlflow.parser.lexer import Lexer, TokenType


class TestConditionalLexer:
    """Test cases for the SQLFlow conditional syntax lexer."""

    def test_if_tokens(self):
        """Test that the lexer correctly tokenizes IF statement tokens."""
        text = """IF ${env} == 'production' THEN
            CREATE TABLE users AS SELECT * FROM prod_users;
        END IF;"""

        lexer = Lexer(text)
        tokens = lexer.tokenize()

        # Filter out whitespace and comments
        tokens = [
            t for t in tokens if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)
        ]

        # Print tokens for debugging
        for i, t in enumerate(tokens):
            print(f"{i}: {t.type} = '{t.value}'")

        # Verify the key tokens are present in the right order
        assert tokens[0].type == TokenType.IF

        # Find the THEN token
        then_tokens = [i for i, t in enumerate(tokens) if t.type == TokenType.THEN]
        assert len(then_tokens) == 1
        then_idx = then_tokens[0]

        # Find the END_IF token
        end_if_tokens = [i for i, t in enumerate(tokens) if t.type == TokenType.END_IF]
        assert len(end_if_tokens) == 1

        # Verify that we have variable and string tokens before THEN
        var_tokens = [
            t
            for t in tokens[:then_idx]
            if t.type == TokenType.VARIABLE
            or (t.type == TokenType.IDENTIFIER and t.value == "env")
        ]
        assert len(var_tokens) > 0

        string_tokens = [t for t in tokens[:then_idx] if t.type == TokenType.STRING]
        assert len(string_tokens) == 1
        assert "production" in string_tokens[0].value

        # Verify tokens after THEN
        create_tokens = [t for t in tokens[then_idx:] if t.type == TokenType.CREATE]
        assert len(create_tokens) == 1

        # Last tokens should be END_IF, SEMICOLON, EOF
        assert tokens[-3].type == TokenType.END_IF
        assert tokens[-2].type == TokenType.SEMICOLON
        assert tokens[-1].type == TokenType.EOF

    def test_if_else_tokens(self):
        """Test that the lexer correctly tokenizes IF-ELSE statement tokens."""
        text = """IF ${env} == 'production' THEN
            CREATE TABLE users AS SELECT * FROM prod_users;
        ELSE
            CREATE TABLE users AS SELECT * FROM dev_users;
        END IF;"""

        lexer = Lexer(text)
        tokens = lexer.tokenize()

        # Filter out whitespace and comments
        tokens = [
            t for t in tokens if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)
        ]

        # Find the ELSE token
        else_tokens = [t for t in tokens if t.type == TokenType.ELSE]
        assert len(else_tokens) == 1

        # Ensure END_IF is present
        end_if_tokens = [t for t in tokens if t.type == TokenType.END_IF]
        assert len(end_if_tokens) == 1

    def test_if_elseif_else_tokens(self):
        """Test that the lexer correctly tokenizes IF-ELSEIF-ELSE statement tokens."""
        text = """IF ${env} == 'production' THEN
            CREATE TABLE users AS SELECT * FROM prod_users;
        ELSE IF ${env} == 'staging' THEN
            CREATE TABLE users AS SELECT * FROM staging_users;
        ELSE
            CREATE TABLE users AS SELECT * FROM dev_users;
        END IF;"""

        lexer = Lexer(text)
        tokens = lexer.tokenize()

        # Filter out whitespace and comments
        tokens = [
            t for t in tokens if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)
        ]

        # Verify presence of ELSE IF token
        else_if_tokens = [t for t in tokens if t.type == TokenType.ELSE_IF]
        assert len(else_if_tokens) == 1

    def test_case_insensitivity(self):
        """Test that the lexer is case-insensitive for conditional keywords."""
        for keyword in ["if", "IF", "If", "iF"]:
            lexer = Lexer(f"{keyword} condition THEN step; END IF;")
            tokens = [
                t
                for t in lexer.tokenize()
                if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)
            ]
            assert tokens[0].type == TokenType.IF

        for keyword in ["else if", "ELSE IF", "Else If", "else IF", "ELSEIF", "elseif"]:
            lexer = Lexer(f"IF cond THEN step; {keyword} other_cond THEN step; END IF;")
            tokens = [
                t
                for t in lexer.tokenize()
                if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)
            ]
            else_if_tokens = [t for t in tokens if t.type == TokenType.ELSE_IF]
            assert len(else_if_tokens) == 1

        for keyword in ["end if", "END IF", "End If", "end IF", "ENDIF", "endif"]:
            lexer = Lexer(f"IF condition THEN step; {keyword};")
            tokens = [
                t
                for t in lexer.tokenize()
                if t.type not in (TokenType.WHITESPACE, TokenType.COMMENT)
            ]
            end_if_tokens = [t for t in tokens if t.type == TokenType.END_IF]
            assert len(end_if_tokens) == 1


class TestConditionalAST:
    """Test cases for the SQLFlow conditional AST nodes."""

    def test_conditional_branch_step(self):
        """Test that ConditionalBranchStep can be instantiated correctly."""
        # Create a SQL step as a nested step
        sql_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM source_table", line_number=2
        )

        # Create a conditional branch
        branch = ConditionalBranchStep(
            condition="${env} == 'production'", steps=[sql_step], line_number=1
        )

        # Verify the branch's attributes
        assert branch.condition == "${env} == 'production'"
        assert len(branch.steps) == 1
        assert isinstance(branch.steps[0], SQLBlockStep)
        assert branch.line_number == 1

        # Verify validation works
        errors = branch.validate()
        assert not errors  # No errors expected

    def test_conditional_block_step(self):
        """Test that ConditionalBlockStep can be instantiated correctly."""
        # Create SQL steps for different branches
        prod_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM prod_table", line_number=2
        )

        staging_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM staging_table", line_number=5
        )

        dev_step = SQLBlockStep(
            table_name="users", sql_query="SELECT * FROM dev_table", line_number=8
        )

        # Create branches
        if_branch = ConditionalBranchStep(
            condition="${env} == 'production'", steps=[prod_step], line_number=1
        )

        elseif_branch = ConditionalBranchStep(
            condition="${env} == 'staging'", steps=[staging_step], line_number=4
        )

        # Create the conditional block
        block = ConditionalBlockStep(
            branches=[if_branch, elseif_branch], else_branch=[dev_step], line_number=1
        )

        # Verify the block's attributes
        assert len(block.branches) == 2
        assert block.branches[0].condition == "${env} == 'production'"
        assert block.branches[1].condition == "${env} == 'staging'"
        assert len(block.else_branch) == 1
        assert isinstance(block.else_branch[0], SQLBlockStep)

        # Verify validation works
        errors = block.validate()
        assert not errors  # No errors expected

    def test_conditional_block_validation_failure(self):
        """Test validation of ConditionalBlockStep with invalid data."""
        # Create a conditional block with no branches (invalid)
        block = ConditionalBlockStep(branches=[], else_branch=None, line_number=1)

        # Validate and expect errors
        errors = block.validate()
        assert errors
        assert any("requires at least one branch" in error for error in errors)

    def test_conditional_branch_validation_failure(self):
        """Test validation of ConditionalBranchStep with invalid data."""
        # Create a branch with empty condition (invalid)
        branch = ConditionalBranchStep(condition="", steps=[], line_number=1)

        # Validate and expect errors
        errors = branch.validate()
        assert errors
        assert any("requires a condition expression" in error for error in errors)

    def test_pipeline_with_conditional_block(self):
        """Test that a Pipeline can contain ConditionalBlockStep."""
        # Create a simple conditional block
        if_branch = ConditionalBranchStep(
            condition="${flag} == true",
            steps=[
                SQLBlockStep(
                    table_name="result", sql_query="SELECT 1 AS value", line_number=2
                )
            ],
            line_number=1,
        )

        block = ConditionalBlockStep(
            branches=[if_branch],
            else_branch=[
                SQLBlockStep(
                    table_name="result", sql_query="SELECT 0 AS value", line_number=5
                )
            ],
            line_number=1,
        )

        # Add to pipeline
        pipeline = Pipeline()
        pipeline.add_step(block)

        # Verify the pipeline contains the block
        assert len(pipeline.steps) == 1
        assert isinstance(pipeline.steps[0], ConditionalBlockStep)

        # Validate the pipeline
        errors = pipeline.validate()
        assert not errors  # No errors expected
