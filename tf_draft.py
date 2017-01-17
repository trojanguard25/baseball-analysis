import pandas as pd
import tensorflow as tf

def build_estimator(model_dir):
    team = tf.contrib.layers.sparse_column_with_hash_bucket("team", hash_bucket_size=40)

    age = tf.contrib.layers.sparse_column_with_keys(column_name="type", keys=["HS", "4Yr", "JC"])

    pos = tf.contrib.layers.sparse_column_with_hash_bucket("pos", hash_bucket_size=20)

    ovpick = tf.contrib.layers.real_valued_column("ovpick")

    wide_columns = [team, age, pos, ovpick,
    tf.contrib.layers.crossed_column([team, age], hash_bucket_size=200),
    tf.contrib.layers.crossed_column([team, pos], hash_bucket_size=1000)]

    m = tf.contrib.learn.LinearRegressor(model_dir=model_dir, feature_columns=wide_columns)

    return m

def input_fn(df):
    continuous_cols = {k: tf.constant(df[k].values) for k in ['ovpick']}

    categorical_cols = {
        k: tf.SparseTensor(
            indices=[[i,0] for i in range(df[k].size)],
            values=df[k].values,
            shape=[df[k].size, 1])
        for k in ['team', 'type', 'pos']}

    feature_cols = dict(continuous_cols)
    feature_cols.update(categorical_cols)

    label = tf.constant(df['total_war'].values)

    return feature_cols, label

def train_and_eval():
    df_train = pd.read_csv(
            tf.gfile.Open('/home/dan/data/draft/team_control_results.csv'),
            names=['ovpick','team','type','pos','total_war'],
            skipinitialspace=True,
            skiprows=1,
            engine="python")

    df_train = df_train.dropna(how='any',axis=0)

    print df_train.dtypes

    model_dir = '/home/dan/data/draft/model'

    m = build_estimator(model_dir)
    m.fit(input_fn=lambda: input_fn(df_train), steps=10)
    results = m.evaluate(input_fn=lambda: input_fn(df_train), steps=1)
    #for val in m.get_variable_names():
        #print("%s: %s" % (val, m.get_variable_value(val)))
    for key in sorted(results):
        print("%s: %s" % (key, results[key]))

    #print m.weights_
    #results = m.predict(input_fn=lambda: input_fn(df_train))

def main(_):
    train_and_eval()

if __name__ == "__main__":
    tf.app.run()

