import matplotlib.pyplot as plt
import seaborn as sns


df = dataset[['R_Score', 'F_Score', 'M_Score', 'Monetary']]
correlation_matrix = df.corr()

plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f")
plt.title('Customer Behavior Correlation')
plt.show()
