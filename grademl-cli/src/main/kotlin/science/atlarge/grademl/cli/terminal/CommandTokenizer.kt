package science.atlarge.grademl.cli.terminal

import java.util.*

object CommandTokenizer {

    fun tokenizeCommand(commandArguments: List<String>): CommandTokenizerResult {
        // Iterate over "words" to parse options and extract arguments
        val tokens = mutableListOf<CommandToken>()
        val remainingWords = LinkedList(commandArguments)
        while (remainingWords.isNotEmpty()) {
            // Interpret the next word
            val nextWord = remainingWords.pop()
            when {
                // Check for invalid option format, starting with more than two dashes
                nextWord.startsWith("---") -> {
                    return CommandTokenizerException(
                        "Invalid format for command option: \"$nextWord\""
                    )
                }
                // Check for a marker closing the option list
                nextWord == "--" -> {
                    remainingWords.mapTo(tokens, ::ArgumentToken)
                    remainingWords.clear()
                }
                // Check for a long option
                nextWord.startsWith("--") -> {
                    val keyValueMatch = nextWord.split('=', limit = 2)
                    tokens.add(LongOptionToken(keyValueMatch[0].trimStart('-')))
                    if (keyValueMatch.size > 1) {
                        tokens.add(ArgumentToken(keyValueMatch[1]))
                    }
                }
                // Check for a short option
                nextWord.startsWith("-") && nextWord != "-" -> {
                    if (nextWord.length > 2) {
                        return CommandTokenizerException("Contraction of short options is not supported: \"$nextWord\"")
                    }
                    tokens.add(ShortOptionToken(nextWord[1]))
                }
                // Treat everything else as an argument token
                else -> tokens.add(ArgumentToken(nextWord))
            }
        }
        return TokenizedCommand(tokens)
    }

}

sealed class CommandToken

data class ShortOptionToken(val option: Char) : CommandToken()

data class LongOptionToken(val option: String) : CommandToken()

data class ArgumentToken(val value: String) : CommandToken()

sealed class CommandTokenizerResult

data class TokenizedCommand(val tokens: List<CommandToken>) : CommandTokenizerResult()

data class CommandTokenizerException(val message: String) : CommandTokenizerResult()