{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "import flask\n",
    "\n",
    "\n",
    "app=flask.Flask(__name__)\n",
    "app.config[\"DEBUG\"]=True\n",
    "\n",
    "@app.route('/',methods=['GET'])\n",
    "def home():\n",
    "    return \"<h1>Main Heading</h1><p> This is paragraph.</p>\"\n",
    "\n",
    "app.run()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
