package reputation.node.reputation;

import dlt.client.tangle.hornet.model.transactions.Transaction;
import dlt.client.tangle.hornet.model.transactions.reputation.Evaluation;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import kmeans.services.KMeansService;
import reputation.node.models.SourceCredibility;
import reputation.node.reputation.credibility.INodeCredibility;
import reputation.node.reputation.credibility.NodeCredibility;


/**
 * Responsável por calcular a reputação de uma coisa, com o uso do algoritmo
 * KMeans.
 *
 * @author Allan Capistrano
 * @version 1.1.0
 */
public class ReputationUsingKMeans implements IReputation {

  private final KMeansService kMeans;
  private final INodeCredibility nodeCredibility;
  private final String sourceId;

  /**
   * Método construtor
   *
   * @param kMeans KMeansService - Referência ao bundle responsável pela execução do
   * algoritmo KMeans.
   * @param nodeCredibility NodeCredibility - Objeto responsável por lidar com
   * a credibilidade dos nós.
   * @param sourceId String - ID do nó avaliador.
   */
  public ReputationUsingKMeans(
    KMeansService kMeans,
    NodeCredibility nodeCredibility,
    String sourceId
  ) {
    this.kMeans = kMeans;
    this.sourceId = sourceId;
    this.nodeCredibility = nodeCredibility;
  }

  /**
   * Calcula a reputação de uma coisa.
   *
   * @param evaluationTransactions List<Transaction> - Lista com as transações
   * de avaliação da coisa.
   * @param useLatestCredibility boolean - Indica se é para usar ou não a
   * credibilidade mais recente para o cálculo da reputação.
   * @param useCredibility boolean - Indica se é para usar ou não a
   * credibilidade no cálculo da reputação.
   * @return Double
   */
  @Override
  public Double calculate(
    List<Transaction> evaluationTransactions,
    boolean useLatestCredibility,
    boolean useCredibility
  ) {
    double reputation = 0.5;

    List<SourceCredibility> nodesCredibilityWithSource =
      this.nodeCredibility.getNodesEvaluatorsCredibility(
          evaluationTransactions,
          this.sourceId,
          true
        );

    if (!nodesCredibilityWithSource.isEmpty()) {
      /* Obtendo somente o valor da credibilidade dos nós avaliadores. */
      List<Float> nodesCredibility = nodesCredibilityWithSource
        .stream()
        .map(SourceCredibility::getCredibility)
        .collect(Collectors.toList());

      /* Executando o algoritmo KMeans. */
      List<Float> kMeansResult = this.kMeans.execute(nodesCredibility);

      /* Obtendo somente os nós que possuem as credibilidades calculadas pelo algoritmo KMeans. */
      List<SourceCredibility> nodesWithHighestCredibilities = nodesCredibilityWithSource
        .stream()
        .filter(node -> kMeansResult.contains(node.getCredibility()))
        .collect(Collectors.toList());

      OptionalDouble temp;

      if (useLatestCredibility) { // Cálculo da reputação usando a credibilidade mais recente.
        /**
         * Calculando a reputação a partir da nota de serviço e das credibilidades mais recentes dos nós avaliadores.
         */

        /* Caso utilize a credibilidade para o cálculo da reputação. */
        if (useCredibility) {
          /* Obtendo somente as transações dos nós possuem as credibilidades calculadas pelo algoritmo KMeans.  */
          List<Transaction> evaluationTransactionsOfNodesWithHighestCredibilities = evaluationTransactions
            .stream()
            .filter(nodeEvaluation ->
              nodesWithHighestCredibilities
                .stream()
                .anyMatch(sourceCredibility ->
                  nodeEvaluation
                    .getSource()
                    .equals(sourceCredibility.getSource())
                )
            )
            .collect(Collectors.toList());

          temp =
            evaluationTransactionsOfNodesWithHighestCredibilities
              .stream()
              .flatMapToDouble(nodeEvaluation ->
                nodesWithHighestCredibilities
                  .stream()
                  .filter(sourceCredibility ->
                    nodeEvaluation
                      .getSource()
                      .equals(sourceCredibility.getSource())
                  )
                  .mapToDouble(sourceCredibility ->
                    sourceCredibility.getCredibility() *
                    ((Evaluation) nodeEvaluation).getServiceEvaluation()
                  )
              )
              .average();
        } else { // Caso não precise da credibilidade.
          temp =
            evaluationTransactions
              .stream()
              .filter(nodeEvaluation ->
                nodesWithHighestCredibilities
                  .stream()
                  .anyMatch(sourceCredibility ->
                    nodeEvaluation
                      .getSource()
                      .equals(sourceCredibility.getSource())
                  )
              )
              .mapToDouble(nodeEvaluation ->
                ((Evaluation) nodeEvaluation).getValue()
              )
              .average();
        }
      } else { // Cálculo da reputação usando a credibilidade no momento da avaliação.
        /* Calculando a reputação a partir da média das avaliações dos nós calculadas pelo algoritmo KMeans. */
        temp =
          evaluationTransactions
            .stream()
            .filter(nodeEvaluation ->
              nodesWithHighestCredibilities
                .stream()
                .anyMatch(sourceCredibility ->
                  nodeEvaluation
                    .getSource()
                    .equals(sourceCredibility.getSource())
                )
            )
            .mapToDouble(nodeEvaluation ->
              ((Evaluation) nodeEvaluation).getValue()
            )
            .average();
      }

      /* Caso existam transações de avaliação, atualiza o valor de R como a média dessas avaliações. */
      if (temp.isPresent()) {
        reputation = temp.getAsDouble();
      }
    }

    return reputation;
  }
}
