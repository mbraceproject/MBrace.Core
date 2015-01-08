using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nessos.MBrace.CSharp
{
    /// <summary>
    /// Represents a type that can hold a value or it might not have one.
    /// </summary>
    [Serializable]
    public class Option<TValue>
    {
        private TValue _value;

        /// <summary>
        /// Returns whether this object has a value or not.
        /// </summary>
        public bool HasValue { get; private set; }

        /// <summary>
        /// Returns the objects value or throws an exception.
        /// </summary>
        public TValue Value
        {
            get
            {
                if (this.HasValue) return _value; else throw new InvalidOperationException("Object is 'None'.");
            }
        }

        /// <summary>
        /// Returns whether this object has a value or not. Stores the value in the out parameter.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool TryGetValue(out TValue value)
        {
            value = _value;
            return this.HasValue;
        }

        internal FSharpOption<TValue> AsFSharpOption()
        {
            return this.HasValue ? FSharpOption<TValue>.Some(this.Value) : FSharpOption<TValue>.None;
        }

        internal static Option<TValue> FromFSharpOption(FSharpOption<TValue> option)
        {
            return FSharpOption<TValue>.get_IsNone(option) ? Option<TValue>.None : Option<TValue>.Some(option.Value);
        }

        /// <summary>
        /// Creates a new Option with the given value.
        /// </summary>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Option<TValue> Some(TValue value)
        {
            return new Option<TValue> { HasValue = true, _value = value };
        }

        /// <summary>
        /// Creates a new Option that has no value.
        /// </summary>
        public static Option<TValue> None
        {
            get { return new Option<TValue> { HasValue = false }; }
        }
    }
}
